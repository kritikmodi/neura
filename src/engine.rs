use crate::{
    config::{Config, Connection},
    connectors::{self, df_error},
    metrics::{Metrics, SourceMetric},
    security,
};
use anyhow::{Context, Result, ensure};
use datafusion::{
    arrow::{datatypes::SchemaRef, record_batch::RecordBatch},
    catalog::{MemorySchemaProvider, SchemaProvider},
    execution::{
        disk_manager::{DiskManagerBuilder, DiskManagerMode},
        memory_pool::{FairSpillPool, MemoryPool},
        runtime_env::RuntimeEnvBuilder,
        session_state::SessionStateBuilder,
    },
    physical_plan::{SendableRecordBatchStream, displayable, stream::RecordBatchStreamAdapter},
    prelude::{SessionConfig, SessionContext},
    sql::sqlparser::{ast::visit_relations, dialect::GenericDialect, parser::Parser},
};
use datafusion_federation::{
    FederatedQueryPlanner, FederatedTableProviderAdaptor,
    sql::{SQLFederationProvider, SQLTableSource},
};
use futures::StreamExt;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Serialize)]
pub struct Diagnostic {
    pub id: String,
    pub sql_sha256: String,
    pub status: String,
    pub elapsed_ms: f64,
    pub rows: u64,
    pub arrow_bytes: u64,
    pub logical_plan: Option<String>,
    pub physical_plan: Option<String>,
    pub sources: BTreeMap<String, SourceMetric>,
    pub error: Option<String>,
    pub memory_pool_reserved_bytes: usize,
}
struct Entry {
    diagnostic: Diagnostic,
    cancel: CancellationToken,
    metrics: Metrics,
    started: Instant,
}
pub struct Engine {
    pub config: Config,
    pool: Arc<dyn MemoryPool>,
    slots: Arc<Semaphore>,
    history: Mutex<BTreeMap<String, Entry>>,
}
pub struct Query {
    pub id: String,
    pub schema: SchemaRef,
    pub stream: SendableRecordBatchStream,
}
struct Lease {
    engine: Arc<Engine>,
    id: String,
    cancel: CancellationToken,
    _permit: OwnedSemaphorePermit,
    done: bool,
}
impl Lease {
    fn finish(&mut self, status: &str, error: Option<String>) {
        self.done = true;
        self.cancel.cancel();
        if let Some(e) = self.engine.history.lock().unwrap().get_mut(&self.id) {
            e.diagnostic.status = status.into();
            e.diagnostic.error = error;
            e.diagnostic.elapsed_ms = e.started.elapsed().as_secs_f64() * 1000.;
            e.diagnostic.sources = e.metrics.snapshot();
            e.diagnostic.memory_pool_reserved_bytes = self.engine.pool.reserved();
        }
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        if !self.done {
            self.finish(
                "cancelled",
                Some("query stream dropped or planning interrupted".into()),
            );
        }
    }
}
impl Engine {
    pub fn new(config: Config) -> Result<Arc<Self>> {
        config.validate()?;
        std::fs::create_dir_all(&config.limits.spill_dir)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(
                &config.limits.spill_dir,
                std::fs::Permissions::from_mode(0o700),
            )?;
        }
        Ok(Arc::new(Self {
            pool: Arc::new(FairSpillPool::new(config.limits.memory_mb * 1024 * 1024)),
            slots: Arc::new(Semaphore::new(config.limits.max_concurrent_queries)),
            config,
            history: Mutex::new(BTreeMap::new()),
        }))
    }
    pub fn cancel(&self, id: &str) -> bool {
        if let Some(e) = self.history.lock().unwrap().get(id)
            && e.diagnostic.status == "running"
        {
            e.cancel.cancel();
            return true;
        }
        false
    }
    pub fn diagnostic(&self, id: &str) -> Option<Diagnostic> {
        self.history.lock().unwrap().get(id).map(|e| {
            let mut d = e.diagnostic.clone();
            d.sources = e.metrics.snapshot();
            if d.status == "running" {
                d.elapsed_ms = e.started.elapsed().as_secs_f64() * 1000.;
            }
            d
        })
    }
    pub fn diagnostics(&self) -> Vec<Diagnostic> {
        let ids: Vec<_> = self.history.lock().unwrap().keys().cloned().collect();
        ids.iter().filter_map(|id| self.diagnostic(id)).collect()
    }
    async fn context(
        &self,
        sql: &str,
        metrics: Metrics,
        cancel: CancellationToken,
        id: &str,
    ) -> Result<SessionContext> {
        let l = &self.config.limits;
        let runtime = Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(self.pool.clone())
                .with_disk_manager_builder(
                    DiskManagerBuilder::default()
                        .with_mode(DiskManagerMode::Directories(vec![
                            l.spill_dir.clone().into(),
                        ]))
                        .with_max_temp_directory_size(l.max_spill_mb * 1024 * 1024),
                )
                .build()?,
        );
        let mut config = SessionConfig::new()
            .with_target_partitions(l.partitions)
            .with_batch_size(l.batch_rows)
            .with_information_schema(true);
        config.options_mut().execution.parquet.pushdown_filters = l.parquet_row_filter_pushdown;
        config.options_mut().execution.parquet.reorder_filters = true;
        config.options_mut().execution.parquet.enable_page_index = true;
        config.options_mut().execution.sort_spill_reservation_bytes =
            l.sort_merge_reservation_mb * 1024 * 1024;
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime)
            .with_default_features()
            .with_optimizer_rules(datafusion_federation::default_optimizer_rules())
            .with_query_planner(Arc::new(FederatedQueryPlanner::new()))
            .build();
        let ctx = SessionContext::new_with_state(state);
        let parsed = Parser::parse_sql(&GenericDialect {}, sql)?;
        let mut used = HashSet::new();
        let _ = visit_relations(&parsed, |r| {
            if let Some(s) = r.0.first() {
                used.insert(s.to_string().trim_matches('"').to_string());
            }
            std::ops::ControlFlow::<()>::Continue(())
        });
        let catalog = ctx
            .catalog("datafusion")
            .context("default catalog missing")?;
        #[cfg(feature = "postgres-wire")]
        if self.config.server.pg_addr.is_some() {
            // Neura exposes no PostgreSQL user-defined, enum, array or composite types.
            let pg_catalog = Arc::new(MemorySchemaProvider::new());
            catalog.register_schema("pg_catalog", pg_catalog.clone())?;
            for (name, cols) in [
                (
                    "pg_type",
                    vec![
                        ("oid", true),
                        ("typname", false),
                        ("typcategory", false),
                        ("typrelid", true),
                        ("typelem", true),
                        ("typtype", false),
                    ],
                ),
                ("pg_class", vec![("oid", true), ("relkind", false)]),
            ] {
                let fields = cols
                    .into_iter()
                    .map(|(n, i)| {
                        datafusion::arrow::datatypes::Field::new(
                            n,
                            if i {
                                datafusion::arrow::datatypes::DataType::Int64
                            } else {
                                datafusion::arrow::datatypes::DataType::Utf8
                            },
                            false,
                        )
                    })
                    .collect::<Vec<_>>();
                let table = Arc::new(datafusion::datasource::MemTable::try_new(
                    Arc::new(datafusion::arrow::datatypes::Schema::new(fields)),
                    vec![vec![]],
                )?);
                pg_catalog.register_table(name.into(), table.clone())?;
                ctx.register_table(name, table)?;
            }
        }
        for source in &self.config.sources {
            let schema = Arc::new(MemorySchemaProvider::new());
            catalog.register_schema(&source.name, schema.clone())?;
            if !used.contains(&source.name) && !used.contains("information_schema") {
                continue;
            }
            match &source.connection {
                Connection::Parquet {} | Connection::S3 { .. } => {
                    for (name, table) in
                        connectors::files::parquet(&ctx, source, metrics.clone()).await?
                    {
                        schema.register_table(
                            name,
                            Arc::new(connectors::instrument::MeasuredTable {
                                inner: table,
                                metrics: metrics.clone(),
                                name: source.name.clone(),
                            }),
                        )?;
                    }
                }
                Connection::Iceberg {} | Connection::Glue { .. } => {
                    for table in &source.tables {
                        let _timer = metrics.timer(&source.name);
                        schema.register_table(
                            table.name.clone(),
                            Arc::new(connectors::instrument::MeasuredTable {
                                inner: connectors::files::iceberg(source, table).await?,
                                metrics: metrics.clone(),
                                name: source.name.clone(),
                            }),
                        )?;
                    }
                }
                _ => {
                    let exec = Arc::new(
                        connectors::Remote::new(
                            source.clone(),
                            metrics.clone(),
                            l.clone(),
                            cancel.clone(),
                            id.into(),
                        )
                        .await?,
                    );
                    let provider = Arc::new(SQLFederationProvider::new(exec));
                    for table in &source.tables {
                        let remote = table.remote.as_ref().context("remote table missing")?;
                        let table_source = SQLTableSource::new_with_schema(
                            provider.clone(),
                            match remote.split('.').collect::<Vec<_>>().as_slice() {
                                [t] => datafusion::common::TableReference::bare(*t),
                                [s, t] => datafusion::common::TableReference::partial(*s, *t),
                                [c, s, t] => datafusion::common::TableReference::full(*c, *s, *t),
                                _ => anyhow::bail!("remote name requires 1-3 components"),
                            }
                            .into(),
                            crate::arrow_util::schema(&table.columns)?,
                        );
                        schema.register_table(
                            table.name.clone(),
                            Arc::new(FederatedTableProviderAdaptor::new(Arc::new(table_source))),
                        )?;
                    }
                }
            }
        }
        Ok(ctx)
    }
    pub async fn query(self: &Arc<Self>, sql: &str) -> Result<Query> {
        self.query_with_id(sql, uuid::Uuid::new_v4().to_string())
            .await
    }
    pub async fn query_with_id(self: &Arc<Self>, sql: &str, id: String) -> Result<Query> {
        security::validate_sql(sql)?;
        uuid::Uuid::parse_str(&id)?;
        let permit = self
            .slots
            .clone()
            .try_acquire_owned()
            .context("query concurrency limit reached")?;
        let started = Instant::now();
        let deadline = tokio::time::Instant::now()
            + Duration::from_secs(self.config.limits.query_timeout_secs);
        let cancel = CancellationToken::new();
        let metrics = Metrics::default();
        {
            let mut h = self.history.lock().unwrap();
            ensure!(!h.contains_key(&id), "query ID already exists");
            if h.len() >= self.config.limits.diagnostic_history
                && let Some(old) = h
                    .iter()
                    .filter(|(_, e)| e.diagnostic.status != "running")
                    .min_by_key(|(_, e)| e.started)
                    .map(|(id, _)| id.clone())
            {
                h.remove(&old);
            }
            h.insert(
                id.clone(),
                Entry {
                    diagnostic: Diagnostic {
                        id: id.clone(),
                        sql_sha256: format!("{:x}", Sha256::digest(sql.as_bytes())),
                        status: "running".into(),
                        elapsed_ms: 0.,
                        rows: 0,
                        arrow_bytes: 0,
                        logical_plan: None,
                        physical_plan: None,
                        sources: BTreeMap::new(),
                        error: None,
                        memory_pool_reserved_bytes: 0,
                    },
                    cancel: cancel.clone(),
                    metrics: metrics.clone(),
                    started,
                },
            );
        }
        let mut lease = Lease {
            engine: self.clone(),
            id: id.clone(),
            cancel: cancel.clone(),
            _permit: permit,
            done: false,
        };
        let planned = tokio::select! {
         _=cancel.cancelled()=>Err(anyhow::anyhow!("query cancelled")),
         r=tokio::time::timeout_at(deadline,async{
          let ctx=self.context(sql,metrics,cancel.clone(),&id).await?;
          let plan=ctx.state().create_logical_plan(sql).await?;security::validate_plan(&plan)?;
          let df=ctx.execute_logical_plan(plan).await?;
          let logical=df.clone().into_optimized_plan()?;
          let physical=df.create_physical_plan().await?;
          if let Some(e)=self.history.lock().unwrap().get_mut(&id){e.diagnostic.logical_plan=Some(format!("{}",logical.display_indent()));e.diagnostic.physical_plan=Some(format!("{}",displayable(physical.as_ref()).indent(true)));}
          let stream=datafusion::physical_plan::execute_stream(physical.clone(),ctx.task_ctx())?;
          Ok::<_,anyhow::Error>((ctx,stream,physical))
         })=>r.unwrap_or_else(|_|Err(anyhow::anyhow!("query deadline exceeded during planning"))),
        };
        let (ctx, mut input, physical) = match planned {
            Ok(x) => x,
            Err(e) => {
                lease.finish("failed", Some(e.to_string()));
                return Err(e);
            }
        };
        let schema = input.schema();
        let out = schema.clone();
        let max = self.config.limits.max_output_mb * 1024 * 1024;
        let output = async_stream::try_stream! {
         let _ctx=ctx;let mut bytes=0;
         loop{
          let result=tokio::select!{
           biased;
           _=cancel.cancelled()=>Some(Err(df_error("query cancelled"))),
           _=tokio::time::sleep_until(deadline)=>Some(Err(df_error("query deadline exceeded"))),
           r=input.next()=>r,
          };
          match result{
           Some(Ok(batch))=>{
            bytes+=batch.get_array_memory_size();
            if bytes>max{lease.finish("failed",Some("output budget exceeded".into()));Err(df_error("output budget exceeded"))?;}
            if let Some(e)=lease.engine.history.lock().unwrap().get_mut(&lease.id){e.diagnostic.rows+=batch.num_rows() as u64;e.diagnostic.arrow_bytes=bytes as u64;}
            yield batch;
           },Some(Err(e))=>{lease.finish("failed",Some(e.to_string()));Err(e)?;},None=>{
             if let Some(e)=lease.engine.history.lock().unwrap().get_mut(&lease.id){e.diagnostic.physical_plan=Some(format!("{}",datafusion::physical_plan::display::DisplayableExecutionPlan::with_metrics(physical.as_ref()).indent(true)));}
             lease.finish("succeeded",None);break;
           }
          }
         }
        };
        Ok(Query {
            id,
            schema,
            stream: Box::pin(RecordBatchStreamAdapter::new(out, output)),
        })
    }
    #[cfg(feature = "postgres-wire")]
    pub async fn describe(self: &Arc<Self>, sql: &str) -> Result<SchemaRef> {
        security::validate_sql(sql)?;
        let _permit = self
            .slots
            .clone()
            .try_acquire_owned()
            .context("query concurrency limit reached")?;
        let token = CancellationToken::new();
        let _guard = token.clone().drop_guard();
        tokio::time::timeout(
            Duration::from_secs(self.config.limits.query_timeout_secs),
            async {
                let ctx = self
                    .context(sql, Metrics::default(), token, "describe")
                    .await?;
                let df = ctx.sql(sql).await?;
                Ok::<_, anyhow::Error>(Arc::new(df.schema().as_arrow().clone()))
            },
        )
        .await?
    }
    pub async fn collect(self: &Arc<Self>, sql: &str) -> Result<(String, Vec<RecordBatch>)> {
        let mut q = self.query(sql).await?;
        let mut b = vec![];
        while let Some(batch) = q.stream.next().await {
            b.push(batch?);
        }
        Ok((q.id, b))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn engine() -> Arc<Engine> {
        Engine::new(toml::from_str("").unwrap()).unwrap()
    }
    #[tokio::test]
    async fn local_query_and_diagnostics() {
        let e = engine();
        let (id, b) = e.collect("SELECT 1+2 AS n").await.unwrap();
        assert_eq!(b[0].num_rows(), 1);
        assert_eq!(e.diagnostic(&id).unwrap().status, "succeeded");
    }
    #[tokio::test]
    async fn drop_cancels_and_releases_slot() {
        let e = engine();
        let q = e.query("SELECT 1").await.unwrap();
        let id = q.id.clone();
        drop(q);
        assert_eq!(e.diagnostic(&id).unwrap().status, "cancelled");
    }
    #[tokio::test]
    async fn rejects_write() {
        assert!(engine().query("CREATE TABLE t AS SELECT 1").await.is_err());
    }
}
