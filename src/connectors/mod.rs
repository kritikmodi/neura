pub mod files;
pub mod instrument;
pub mod postgres;
pub mod warehouse;
use crate::{
    config::{Connection, Limits, Source},
    metrics::Metrics,
};
use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef,
    common::{Statistics, stats::Precision},
    error::{DataFusionError, Result},
    logical_expr::LogicalPlan,
    physical_plan::{PhysicalExpr, SendableRecordBatchStream},
    sql::{
        sqlparser::{
            ast::{self, TableFactor, VisitMut, VisitorMut},
            dialect::{GenericDialect, SnowflakeDialect},
            parser::Parser,
        },
        unparser::{
            Unparser,
            dialect::{
                BigQueryDialect, CustomDialectBuilder, DefaultDialect, Dialect, PostgreSqlDialect,
            },
        },
    },
};
use datafusion_federation::sql::{AstAnalyzer, SQLExecutor};
use std::{ops::ControlFlow, sync::Arc};
use tokio_util::sync::CancellationToken;

pub fn df_error(e: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Execution(e.to_string())
}
#[derive(Clone)]
pub struct Remote {
    pub source: Source,
    pub pg: Option<Arc<postgres::Session>>,
    pub http: warehouse::Http,
    pub metrics: Metrics,
    pub limits: Limits,
    pub cancel: CancellationToken,
    pub context: String,
}
impl Remote {
    pub async fn new(
        source: Source,
        metrics: Metrics,
        limits: Limits,
        cancel: CancellationToken,
        context: String,
    ) -> anyhow::Result<Self> {
        let pg = if let Connection::Postgres {
            dsn_env,
            allow_insecure,
        } = &source.connection
        {
            Some(
                postgres::Session::connect(dsn_env, *allow_insecure, &limits, cancel.clone())
                    .await?,
            )
        } else {
            None
        };
        Ok(Self {
            source,
            pg,
            http: warehouse::Http::new(limits.max_remote_page_mb)?,
            metrics,
            limits,
            cancel,
            context,
        })
    }
}
#[async_trait]
impl SQLExecutor for Remote {
    fn name(&self) -> &str {
        self.source.connection.kind()
    }
    fn compute_context(&self) -> Option<String> {
        Some(format!("{}:{}", self.context, self.source.name))
    }
    fn dialect(&self) -> Arc<dyn Dialect> {
        match self.source.connection {
            Connection::Postgres { .. } => Arc::new(PostgreSqlDialect {}),
            Connection::Bigquery { .. } => Arc::new(BigQueryDialect {}),
            Connection::Databricks { .. } => Arc::new(
                CustomDialectBuilder::new()
                    .with_identifier_quote_style('`')
                    .build(),
            ),
            _ => Arc::new(DefaultDialect {}),
        }
    }
    fn ast_analyzer(&self) -> Option<AstAnalyzer> {
        if self.pg.is_some() {
            return None;
        }
        let source = self.source.clone();
        Some(Box::new(move |mut statement| {
            struct Snap<'a> {
                source: &'a Source,
            }
            impl VisitorMut for Snap<'_> {
                type Break = DataFusionError;
                fn post_visit_table_factor(
                    &mut self,
                    f: &mut TableFactor,
                ) -> ControlFlow<Self::Break> {
                    if let TableFactor::Table { name, version, .. } = f {
                        let key = name
                            .0
                            .iter()
                            .map(|i| i.to_string().trim_matches(['`', '"']).to_string())
                            .collect::<Vec<_>>()
                            .join(".");
                        if let Some(t) = self
                            .source
                            .tables
                            .iter()
                            .find(|t| t.remote.as_deref() == Some(&key))
                        {
                            let snap = t.snapshot.as_deref().unwrap_or("");
                            let clause = match self.source.connection {
                                Connection::Databricks { .. } => format!("VERSION AS OF {snap}"),
                                Connection::Bigquery { .. } => {
                                    format!("FOR SYSTEM_TIME AS OF TIMESTAMP '{snap}'")
                                }
                                Connection::Snowflake { .. } => {
                                    format!("AT(TIMESTAMP => '{snap}'::TIMESTAMP_TZ)")
                                }
                                _ => return ControlFlow::Continue(()),
                            };
                            let dialect: &dyn datafusion::sql::sqlparser::dialect::Dialect =
                                match self.source.connection {
                                    Connection::Snowflake { .. } => &SnowflakeDialect {},
                                    Connection::Databricks { .. } => {
                                        &datafusion::sql::sqlparser::dialect::DatabricksDialect {}
                                    }
                                    Connection::Bigquery { .. } => {
                                        &datafusion::sql::sqlparser::dialect::BigQueryDialect {}
                                    }
                                    _ => &GenericDialect {},
                                };
                            match Parser::parse_sql(dialect, &format!("SELECT * FROM t {clause}")) {
                                Ok(v) => {
                                    if let ast::Statement::Query(q) = &v[0]
                                        && let ast::SetExpr::Select(s) = q.body.as_ref()
                                        && let TableFactor::Table { version: v, .. } =
                                            &s.from[0].relation
                                    {
                                        *version = v.clone();
                                        return ControlFlow::Continue(());
                                    }
                                    return ControlFlow::Break(df_error(
                                        "unable to apply snapshot",
                                    ));
                                }
                                Err(e) => return ControlFlow::Break(df_error(e)),
                            }
                        }
                    }
                    ControlFlow::Continue(())
                }
            }
            if let ControlFlow::Break(e) = statement.visit(&mut Snap { source: &source }) {
                return Err(e);
            }
            Ok(statement)
        }))
    }
    fn execute(
        &self,
        query: &str,
        schema: SchemaRef,
        _filters: &[Arc<dyn PhysicalExpr>],
    ) -> Result<SendableRecordBatchStream> {
        if let Some(pg) = &self.pg {
            postgres::execute(
                pg.clone(),
                query.to_string(),
                schema,
                self.metrics.clone(),
                self.source.name.clone(),
                self.limits.clone(),
                self.cancel.clone(),
            )
        } else {
            warehouse::execute(self.clone(), query.to_string(), schema)
        }
    }
    async fn table_names(&self) -> Result<Vec<String>> {
        Ok(self
            .source
            .tables
            .iter()
            .filter_map(|t| t.remote.clone())
            .collect())
    }
    async fn get_table_schema(&self, name: &str) -> Result<SchemaRef> {
        let t = self
            .source
            .tables
            .iter()
            .find(|t| t.remote.as_deref() == Some(name))
            .ok_or_else(|| df_error("unknown table"))?;
        crate::arrow_util::schema(&t.columns).map_err(df_error)
    }
    async fn statistics(&self, plan: &LogicalPlan) -> Result<Statistics> {
        let mut stats = Statistics::new_unknown(plan.schema().as_arrow());
        if let Some(pg) = &self.pg {
            let sql = {
                let dialect = PostgreSqlDialect {};
                Unparser::new(&dialect)
                    .plan_to_sql(plan)
                    .map(|mut statement| {
                        let _ = ast::visit_relations_mut(&mut statement, |name| {
                            let key = name
                                .0
                                .iter()
                                .map(|i| i.to_string().trim_matches('"').to_string())
                                .collect::<Vec<_>>()
                                .join(".");
                            if let Some(t) = self
                                .source
                                .tables
                                .iter()
                                .find(|t| key == format!("{}.{}", self.source.name, t.name))
                                && let Ok(parsed) = Parser::parse_sql(
                                    &GenericDialect {},
                                    &format!("SELECT * FROM {}", t.remote.as_deref().unwrap()),
                                )
                                && let ast::Statement::Query(q) = &parsed[0]
                                && let ast::SetExpr::Select(s) = q.body.as_ref()
                                && let TableFactor::Table { name: n, .. } = &s.from[0].relation
                            {
                                *name = n.clone();
                            }
                            ControlFlow::<()>::Continue(())
                        });
                        statement.to_string()
                    })
            };
            if let Ok(stmt) = sql {
                let _timer = self.metrics.timer(&self.source.name);
                let client = pg.client.lock().await;
                if client
                    .batch_execute("SAVEPOINT neura_estimate")
                    .await
                    .is_err()
                {
                    return Ok(stats);
                }
                if let Ok(rows) = client
                    .query(&format!("EXPLAIN (FORMAT JSON) {stmt}"), &[])
                    .await
                    && let Some(row) = rows.first()
                    && let Ok(v) = row.try_get::<_, serde_json::Value>(0)
                {
                    let p = &v[0]["Plan"];
                    if let Some(n) = p["Plan Rows"].as_u64() {
                        stats.num_rows = Precision::Inexact(n as usize);
                        stats.total_byte_size = Precision::Inexact(
                            (n * p["Plan Width"].as_u64().unwrap_or(0)) as usize,
                        );
                    }
                }
                // A failed optional estimate must not poison the read transaction.
                let _ = client
                    .batch_execute(
                        "ROLLBACK TO SAVEPOINT neura_estimate; RELEASE SAVEPOINT neura_estimate",
                    )
                    .await;
            }
        }
        Ok(stats)
    }
}

#[cfg(test)]
mod snapshot_tests {
    use super::*;
    #[tokio::test]
    async fn warehouse_snapshot_clauses_are_injected() {
        for (kind, extra, snapshot, expected) in [
            (
                "databricks",
                "host='https://example.cloud.databricks.com'\nwarehouse_id='w'\ntoken_env='UNUSED'",
                "42",
                "VERSION AS OF 42",
            ),
            (
                "snowflake",
                "host='https://example.snowflakecomputing.com'\nwarehouse='w'\ndatabase='d'\nschema='s'\nrole='reader'\ntoken_env='UNUSED'",
                "2026-01-01T00:00:00Z",
                "AT(TIMESTAMP =>",
            ),
            (
                "bigquery",
                "project='test-project'\nlocation='US'\ntoken_env='UNUSED'\nmaximum_bytes_billed=1000",
                "2026-01-01T00:00:00Z",
                "FOR SYSTEM_TIME AS OF",
            ),
        ] {
            let source:Source=toml::from_str(&format!("name='test'\nkind='{kind}'\n{extra}\n[[tables]]\nname='t'\nremote='db.tbl'\nsnapshot='{snapshot}'\ncolumns=[{{name='id',data_type='int64'}}]")).unwrap();
            let remote = Remote::new(
                source,
                Metrics::default(),
                Limits::default(),
                CancellationToken::new(),
                "test".into(),
            )
            .await
            .unwrap();
            let mut rewrite = remote.ast_analyzer().unwrap();
            let ast = Parser::parse_sql(
                &GenericDialect {},
                "SELECT * FROM db.tbl a JOIN db.tbl b ON a.id=b.id",
            )
            .unwrap()
            .remove(0);
            let sql = rewrite(ast).unwrap().to_string();
            assert_eq!(sql.matches(expected).count(), 2, "{sql}");
        }
    }
}
