//! PostgreSQL v3 simple/extended protocol for analytical clients. No write protocol.
use crate::{
    config::secret,
    engine::{Engine, Query},
};
use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use datafusion::arrow::{
    array::*,
    datatypes::{DataType, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::sql::sqlparser::{
    ast::{Expr, Value, VisitMut, VisitorMut, visit_expressions},
    dialect::PostgreSqlDialect,
    parser::Parser,
};
use futures::StreamExt;
use pgwire::{
    api::{
        ClientInfo, ClientPortalStore, ConnectionManager, PgWireServerHandlers, Type,
        auth::{
            AuthSource, DefaultServerParameterProvider, LoginInfo, Password, StartupHandler,
            sasl::{
                SASLAuthStartupHandler,
                scram::{ScramAuth, gen_salted_password},
            },
        },
        cancel::DefaultCancelHandler,
        portal::{Format, Portal},
        query::{ExtendedQueryHandler, SimpleQueryHandler},
        results::{FieldInfo, QueryResponse, Response, Tag},
        stmt::QueryParser,
    },
    error::{ErrorInfo, PgWireError, PgWireResult},
    messages::data::DataRow,
};
use std::{ops::ControlFlow, sync::Arc, time::Duration};
fn err(e: impl std::fmt::Display) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".into(),
        "0A000".into(),
        e.to_string(),
    )))
}
struct Auth {
    user: String,
    password: String,
}
impl std::fmt::Debug for Auth {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("Auth([redacted])")
    }
}
#[async_trait]
impl AuthSource for Auth {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        if login.user() != Some(&self.user) {
            return Err(err("authentication failed"));
        }
        let salt = uuid::Uuid::new_v4().as_bytes().to_vec();
        let hash = gen_salted_password(&self.password, &salt, 4096);
        Ok(Password::new(Some(salt), hash))
    }
}
#[derive(Clone)]
pub struct Prepared {
    sql: String,
    types: Vec<Type>,
    schema: SchemaRef,
}
pub struct SqlParser {
    engine: Arc<Engine>,
}
fn bind_sql(sql: &str, values: &[Option<String>]) -> PgWireResult<String> {
    let mut ast = Parser::parse_sql(&PostgreSqlDialect {}, sql).map_err(err)?;
    if ast.len() != 1 {
        return Err(err("one statement required"));
    }
    struct Bind<'a>(&'a [Option<String>]);
    impl VisitorMut for Bind<'_> {
        type Break = PgWireError;
        fn post_visit_expr(&mut self, e: &mut Expr) -> ControlFlow<Self::Break> {
            if let Expr::Value(v) = e
                && let Value::Placeholder(p) = &v.value
            {
                let Some(index) = p
                    .strip_prefix('$')
                    .and_then(|v| v.parse::<usize>().ok())
                    .filter(|i| *i > 0)
                else {
                    return ControlFlow::Break(err("invalid parameter"));
                };
                let Some(value) = self.0.get(index - 1) else {
                    return ControlFlow::Break(err("missing parameter"));
                };
                v.value = match value {
                    Some(s) => Value::SingleQuotedString(s.clone()),
                    None => Value::Null,
                };
            }
            ControlFlow::Continue(())
        }
    }
    if let ControlFlow::Break(e) = ast.visit(&mut Bind(values)) {
        return Err(e);
    }
    Ok(ast[0].to_string())
}
#[async_trait]
impl QueryParser for SqlParser {
    type Statement = Prepared;
    async fn parse_sql<C>(&self, _: &C, sql: &str, types: &[Option<Type>]) -> PgWireResult<Prepared>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        crate::security::validate_sql(sql).map_err(err)?;
        let ast = Parser::parse_sql(&PostgreSqlDialect {}, sql).map_err(err)?;
        let mut n = 0;
        let _ = visit_expressions(&ast, |e| {
            if let Expr::Value(v) = e
                && let Value::Placeholder(p) = &v.value
            {
                n = n.max(p.trim_start_matches('$').parse::<usize>().unwrap_or(0));
            }
            ControlFlow::<()>::Continue(())
        });
        if n > 1024 {
            return Err(err("too many parameters"));
        }
        let typed = (0..n)
            .map(|i| types.get(i).and_then(Clone::clone).unwrap_or(Type::TEXT))
            .collect::<Vec<_>>();
        let text = bind_sql(sql, &vec![None; n])?;
        let schema = self.engine.describe(&text).await.map_err(err)?;
        Ok(Prepared {
            sql: sql.into(),
            types: typed,
            schema,
        })
    }
    fn get_parameter_types(&self, s: &Prepared) -> PgWireResult<Vec<Type>> {
        Ok(s.types.clone())
    }
    fn get_result_schema(
        &self,
        s: &Prepared,
        format: Option<&Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        fields(&s.schema, format.unwrap_or(&Format::UnifiedText))
    }
}
fn pg_type(t: &DataType) -> PgWireResult<Type> {
    Ok(match t {
        DataType::Boolean => Type::BOOL,
        DataType::Int16 => Type::INT2,
        DataType::Int32 => Type::INT4,
        DataType::Int64 | DataType::UInt64 => Type::INT8,
        DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        DataType::Decimal128(..) | DataType::Decimal256(..) => Type::NUMERIC,
        DataType::Date32 => Type::DATE,
        DataType::Timestamp(_, None) => Type::TIMESTAMP,
        DataType::Timestamp(_, Some(_)) => Type::TIMESTAMPTZ,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null => Type::TEXT,
        _ => return Err(err(format!("unsupported PostgreSQL result type {t}"))),
    })
}
fn fields(s: &SchemaRef, format: &Format) -> PgWireResult<Vec<FieldInfo>> {
    if let Format::Individual(f) = format
        && f.len() != s.fields().len()
    {
        return Err(err("result format count mismatch"));
    }
    s.fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            Ok(FieldInfo::new(
                f.name().clone(),
                None,
                None,
                pg_type(f.data_type())?,
                format.format_for(i),
            ))
        })
        .collect()
}
fn binary(a: &ArrayRef, i: usize) -> PgWireResult<Vec<u8>> {
    macro_rules! primitive {
        ($ty:ty) => {
            a.as_any()
                .downcast_ref::<$ty>()
                .unwrap()
                .value(i)
                .to_be_bytes()
                .to_vec()
        };
    }
    Ok(match a.data_type() {
        DataType::Boolean => vec![u8::from(
            a.as_any().downcast_ref::<BooleanArray>().unwrap().value(i),
        )],
        DataType::Int16 => primitive!(Int16Array),
        DataType::Int32 => primitive!(Int32Array),
        DataType::Int64 => primitive!(Int64Array),
        DataType::Float32 => primitive!(Float32Array),
        DataType::Float64 => primitive!(Float64Array),
        DataType::Date32 => (a.as_any().downcast_ref::<Date32Array>().unwrap().value(i) - 10957)
            .to_be_bytes()
            .to_vec(),
        DataType::Timestamp(_, _) => {
            let target =
                DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None);
            let cast = datafusion::arrow::compute::cast(a, &target).map_err(err)?;
            (cast
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .value(i)
                - 946684800000000i64)
                .to_be_bytes()
                .to_vec()
        }
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => {
            datafusion::arrow::util::display::array_value_to_string(a, i)
                .map_err(err)?
                .into_bytes()
        }
        _ => {
            return Err(err(
                "binary result format unsupported for this type; request text results",
            ));
        }
    })
}
fn encode(batch: &RecordBatch, i: usize, format: &Format) -> PgWireResult<DataRow> {
    let mut buf = BytesMut::new();
    for (j, a) in batch.columns().iter().enumerate() {
        if a.is_null(i) {
            buf.put_i32(-1);
        } else {
            let b = if format.is_binary(j) {
                binary(a, i)?
            } else {
                datafusion::arrow::util::display::array_value_to_string(a, i)
                    .map_err(err)?
                    .into_bytes()
            };
            buf.put_i32(b.len() as i32);
            buf.extend_from_slice(&b);
        }
    }
    Ok(DataRow::new(buf, batch.num_columns() as i16))
}
struct Timeout(u64);
pub struct Handler {
    engine: Arc<Engine>,
    parser: Arc<SqlParser>,
}
impl Handler {
    async fn run(
        &self,
        sql: &str,
        format: Format,
        timeout_ms: Option<u64>,
    ) -> PgWireResult<Response> {
        let timeout = Duration::from_millis(
            timeout_ms
                .filter(|v| *v > 0)
                .unwrap_or(self.engine.config.limits.query_timeout_secs * 1000)
                .min(self.engine.config.limits.query_timeout_secs * 1000),
        );
        let deadline = tokio::time::Instant::now() + timeout;
        let mut q: Query = tokio::time::timeout_at(deadline, self.engine.query(sql))
            .await
            .map_err(|_| err("statement timeout"))?
            .map_err(err)?;
        let schema = Arc::new(fields(&q.schema, &format)?);
        let stream = async_stream::try_stream! {
         loop{
          let next=tokio::time::timeout_at(deadline,q.stream.next()).await.map_err(|_|err("statement timeout"))?;
          let Some(b)=next else{break;};let b=b.map_err(err)?;
          for i in 0..b.num_rows(){yield encode(&b,i,&format)?;}
         }
        };
        Ok(Response::Query(QueryResponse::new(schema, stream)))
    }
}
#[async_trait]
impl SimpleQueryHandler for Handler {
    async fn do_query<C>(&self, c: &mut C, sql: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Unpin + Send + Sync,
    {
        let s = sql.trim().trim_end_matches(';').trim();
        let lower = s.to_ascii_lowercase();
        if matches!(
            lower.as_str(),
            "set time zone 'utc'"
                | "set timezone = 'utc'"
                | "set client_encoding to 'utf8'"
                | "set client_encoding = 'utf8'"
        ) {
            return Ok(vec![Response::Execution(Tag::new("SET"))]);
        }
        if let Some(n) = lower
            .strip_prefix("set statement_timeout to ")
            .or_else(|| lower.strip_prefix("set statement_timeout = "))
        {
            let ms = n.trim_matches('\'').parse::<u64>().map_err(err)?;
            c.session_extensions().insert(Timeout(ms));
            return Ok(vec![Response::Execution(Tag::new("SET"))]);
        }
        let rewrite = match lower.as_str() {
            "select version()" => Some("SELECT 'Neura 0.1 (PostgreSQL protocol 3)' AS version"),
            "show transaction_read_only" => Some("SELECT 'on' AS transaction_read_only"),
            "show timezone" => Some("SELECT 'UTC' AS TimeZone"),
            _ => None,
        };
        Ok(vec![
            self.run(
                rewrite.unwrap_or(sql),
                Format::UnifiedText,
                c.session_extensions().get::<Timeout>().map(|v| v.0),
            )
            .await?,
        ])
    }
}
#[async_trait]
impl ExtendedQueryHandler for Handler {
    type Statement = Prepared;
    type QueryParser = SqlParser;
    fn query_parser(&self) -> Arc<SqlParser> {
        self.parser.clone()
    }
    async fn do_query<C>(
        &self,
        c: &mut C,
        p: &Portal<Prepared>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Unpin + Send + Sync,
    {
        if p.parameters.len() != p.statement.statement.types.len() {
            return Err(err("parameter count mismatch"));
        }
        let mut values = vec![];
        for (i, t) in p.statement.statement.types.iter().enumerate() {
            let value = if p.parameter_format.is_text(i) {
                p.parameters[i]
                    .as_ref()
                    .map(|b| std::str::from_utf8(b).map(str::to_owned))
                    .transpose()
                    .map_err(err)?
            } else {
                match *t {
                    Type::INT2 => p.parameter::<i16>(i, t)?.map(|v| v.to_string()),
                    Type::INT4 => p.parameter::<i32>(i, t)?.map(|v| v.to_string()),
                    Type::INT8 => p.parameter::<i64>(i, t)?.map(|v| v.to_string()),
                    Type::BOOL => p.parameter::<bool>(i, t)?.map(|v| v.to_string()),
                    Type::FLOAT4 => p.parameter::<f32>(i, t)?.map(|v| v.to_string()),
                    Type::FLOAT8 => p.parameter::<f64>(i, t)?.map(|v| v.to_string()),
                    Type::TEXT | Type::VARCHAR => p.parameter::<String>(i, t)?,
                    _ => return Err(err("unsupported binary parameter type")),
                }
            };
            values.push(value);
        }
        let sql = bind_sql(&p.statement.statement.sql, &values)?;
        self.run(
            &sql,
            p.result_column_format.clone(),
            c.session_extensions().get::<Timeout>().map(|v| v.0),
        )
        .await
    }
}
struct Factory {
    handler: Arc<Handler>,
    auth: Arc<Auth>,
    manager: Arc<ConnectionManager>,
}
impl PgWireServerHandlers for Factory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.handler.clone()
    }
    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.handler.clone()
    }
    fn cancel_handler(&self) -> Arc<impl pgwire::api::cancel::CancelHandler> {
        Arc::new(DefaultCancelHandler::new(self.manager.clone()))
    }
    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        let mut params = DefaultServerParameterProvider::default();
        params.server_version = "16.6-neura-0.1".into();
        params.is_superuser = false;
        params.default_transaction_read_only = true;
        Arc::new(
            SASLAuthStartupHandler::new(Arc::new(params))
                .with_scram(ScramAuth::new(self.auth.clone()))
                .with_connection_manager(self.manager.clone()),
        )
    }
}
pub async fn serve(engine: Arc<Engine>) -> anyhow::Result<()> {
    let password = secret(&engine.config.server.pg_password_env)?;
    anyhow::ensure!(
        password.len() >= 16,
        "PostgreSQL password must be at least 16 bytes"
    );
    let auth = Arc::new(Auth {
        user: engine.config.server.pg_user.clone(),
        password,
    });
    let listener = tokio::net::TcpListener::bind(&engine.config.server.pg_addr).await?;
    let slots = Arc::new(tokio::sync::Semaphore::new(
        engine.config.limits.max_connections,
    ));
    let factory = Arc::new(Factory {
        handler: Arc::new(Handler {
            parser: Arc::new(SqlParser {
                engine: engine.clone(),
            }),
            engine,
        }),
        auth,
        manager: Arc::new(ConnectionManager::new()),
    });
    loop {
        let (socket, _) = listener.accept().await?;
        let Ok(permit) = slots.clone().try_acquire_owned() else {
            drop(socket);
            continue;
        };
        let factory = factory.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let _ = pgwire::tokio::process_socket(socket, None, factory).await;
        });
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parameters_cannot_inject() {
        let sql = bind_sql(
            "SELECT $1 AS x, '$1' AS literal",
            &[Some("'; DROP TABLE t; --".into())],
        )
        .unwrap();
        assert_eq!(
            Parser::parse_sql(&PostgreSqlDialect {}, &sql)
                .unwrap()
                .len(),
            1
        );
        assert!(sql.contains("'$1' AS literal"));
    }
}
