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
    pub file_cache_resident_bytes: usize,
    /// Fixed target chosen when this query was admitted, not a thread count.
    pub selected_partitions: usize,
    pub phases: PhaseTimings,
}
/// Wall-clock phases, not CPU time. Execution includes consumer backpressure;
/// cleanup ends successful PostgreSQL snapshots before returning connections.
#[derive(Clone, Default, Serialize)]
pub struct PhaseTimings {
    pub catalog_ms: f64,
    pub logical_plan_ms: f64,
    pub optimize_ms: f64,
    pub physical_plan_ms: f64,
    pub execution_ms: f64,
    pub cleanup_ms: f64,
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
    file_cache: Option<Arc<connectors::cache::ReadCache>>,
    postgres_pools: BTreeMap<String, Arc<connectors::postgres::Pool>>,
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
            e.diagnostic.file_cache_resident_bytes = self.engine.cache_resident_bytes();
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

#[derive(Default)]
struct TableSelection {
    all: bool,
    tables: BTreeMap<String, HashSet<String>>,
}
impl TableSelection {
    fn parse(sql: &str) -> Result<Self> {
        let parsed = Parser::parse_sql(&GenericDialect {}, sql)?;
        let mut selected = Self::default();
        let _ = visit_relations(&parsed, |relation| {
            let parts = relation
                .0
                .iter()
                .map(|part| {
                    part.as_ident().map(|ident| {
                        if ident.quote_style.is_some() {
                            ident.value.clone()
                        } else {
                            ident.value.to_ascii_lowercase()
                        }
                    })
                })
                .collect::<Option<Vec<_>>>();
            match parts.as_deref() {
                Some([schema, table]) => selected.add(schema, table),
                Some([catalog, schema, table]) if catalog == "datafusion" => {
                    selected.add(schema, table)
                }
                // Bare names resolve in public (or to a CTE), not in a source
                // schema. The visitor also traverses every nested CTE/subquery.
                Some([_]) | Some([_, _, _]) => {}
                // Unknown identifier syntax stays conservative. DataFusion must
                // still resolve and validate it; no permissions are inferred.
                _ => selected.all = true,
            }
            std::ops::ControlFlow::<()>::Continue(())
        });
        Ok(selected)
    }
    fn add(&mut self, schema: &str, table: &str) {
        if schema == "information_schema" {
            // Introspection must enumerate the whole configured catalog.
            self.all = true;
        } else {
            self.tables
                .entry(schema.into())
                .or_default()
                .insert(table.into());
        }
    }
    fn includes(&self, source: &str, table: &str) -> bool {
        self.all || self.tables.get(source).is_some_and(|t| t.contains(table))
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
        let file_cache = (config.limits.file_cache_mb > 0).then(|| {
            Arc::new(connectors::cache::ReadCache::new(
                config.limits.file_cache_mb * 1024 * 1024,
                config.limits.file_cache_max_object_mb * 1024 * 1024,
            ))
        });
        let postgres_pools = config
            .sources
            .iter()
            .filter(|s| matches!(s.connection, Connection::Postgres { .. }))
            .map(|s| {
                (
                    s.name.clone(),
                    connectors::postgres::Pool::new(config.limits.max_concurrent_queries),
                )
            })
            .collect();
        Ok(Arc::new(Self {
            pool: Arc::new(FairSpillPool::new(
                (config.limits.memory_mb - config.limits.file_cache_mb) * 1024 * 1024,
            )),
            file_cache,
            postgres_pools,
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
                d.memory_pool_reserved_bytes = self.pool.reserved();
                d.file_cache_resident_bytes = self.cache_resident_bytes();
            }
            d
        })
    }
    pub fn diagnostics(&self) -> Vec<Diagnostic> {
        let ids: Vec<_> = self.history.lock().unwrap().keys().cloned().collect();
        ids.iter().filter_map(|id| self.diagnostic(id)).collect()
    }
    fn cache_resident_bytes(&self) -> usize {
        self.file_cache.as_ref().map_or(0, |c| c.resident_bytes())
    }
    fn record_phase(&self, id: &str, update: impl FnOnce(&mut PhaseTimings)) {
        if let Some(entry) = self.history.lock().unwrap().get_mut(id) {
            update(&mut entry.diagnostic.phases);
        }
    }
    fn admitted_partitions(&self) -> usize {
        let limits = &self.config.limits;
        if !limits.adaptive_partitions {
            return limits.partitions;
        }
        // This is a planning heuristic, not a global CPU semaphore. Existing
        // plans retain their targets; I/O-bound and backpressured queries also
        // hold admission slots. The hard query and memory limits are unchanged.
        let admitted = limits
            .max_concurrent_queries
            .saturating_sub(self.slots.available_permits())
            .max(1);
        (limits.partitions / admitted).max(1)
    }
    async fn context(
        &self,
        sql: &str,
        metrics: Metrics,
        cancel: CancellationToken,
        id: &str,
        partitions: usize,
    ) -> Result<(SessionContext, Vec<Arc<connectors::postgres::Session>>)> {
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
            .with_target_partitions(partitions)
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
        let selection = TableSelection::parse(sql)?;
        let read_ahead = (l.s3_read_ahead_kb > 0).then(|| {
            (
                Arc::new(connectors::files::ReadAheadBudget::new(
                    self.pool.clone(),
                    l.s3_read_ahead_budget_mb * 1024 * 1024,
                )),
                l.s3_read_ahead_kb * 1024,
            )
        });
        let mut postgres_sessions = Vec::new();
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
            let mut selected = source.clone();
            selected
                .tables
                .retain(|t| selection.includes(&source.name, &t.name));
            if selected.tables.is_empty() {
                continue;
            }
            let source = &selected;
            match &source.connection {
                Connection::Parquet {} | Connection::S3 { .. } => {
                    for (name, table) in connectors::files::parquet(
                        &ctx,
                        source,
                        metrics.clone(),
                        read_ahead.clone(),
                        self.file_cache.clone(),
                    )
                    .await?
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
                        connectors::Remote::new_pooled(
                            source.clone(),
                            metrics.clone(),
                            l.clone(),
                            cancel.clone(),
                            id.into(),
                            self.postgres_pools.get(&source.name).cloned(),
                        )
                        .await?,
                    );
                    if let Some(session) = &exec.pg {
                        postgres_sessions.push(session.clone());
                    }
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
        Ok((ctx, postgres_sessions))
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
        let selected_partitions = self.admitted_partitions();
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
                        file_cache_resident_bytes: 0,
                        selected_partitions,
                        phases: PhaseTimings::default(),
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
          let phase = Instant::now();
          let context = self.context(sql,metrics,cancel.clone(),&id,selected_partitions).await;
          self.record_phase(&id, |p| p.catalog_ms = phase.elapsed().as_secs_f64() * 1000.);
          let (ctx, postgres_sessions) = context?;
          let phase = Instant::now();
          let plan=ctx.state().create_logical_plan(sql).await;
          self.record_phase(&id, |p| p.logical_plan_ms = phase.elapsed().as_secs_f64() * 1000.);
          let plan = plan?;
          security::validate_plan(&plan)?;
          let df=ctx.execute_logical_plan(plan).await?;
          // Keep the DataFrame's session snapshot (including now()) and optimize
          // exactly once. SessionState::create_physical_plan would optimize again.
          let (state, plan) = df.into_parts();
          let phase = Instant::now();
          let logical = state.optimize(&plan);
          self.record_phase(&id, |p| p.optimize_ms = phase.elapsed().as_secs_f64() * 1000.);
          let logical = logical?;
          let phase = Instant::now();
          let physical = state.query_planner().create_physical_plan(&logical, &state).await;
          self.record_phase(&id, |p| p.physical_plan_ms = phase.elapsed().as_secs_f64() * 1000.);
          let physical = physical?;
          if let Some(e)=self.history.lock().unwrap().get_mut(&id){e.diagnostic.logical_plan=Some(format!("{}",logical.display_indent()));e.diagnostic.physical_plan=Some(format!("{}",displayable(physical.as_ref()).indent(true)));}
          let stream=datafusion::physical_plan::execute_stream(physical.clone(),state.task_ctx())?;
          Ok::<_,anyhow::Error>((ctx,stream,physical,postgres_sessions))
         })=>r.unwrap_or_else(|_|Err(anyhow::anyhow!("query deadline exceeded during planning"))),
        };
        let (ctx, mut input, physical, postgres_sessions) = match planned {
            Ok(x) => x,
            Err(e) => {
                lease.finish("failed", Some(e.to_string()));
                return Err(e);
            }
        };
        let schema = input.schema();
        let out = schema.clone();
        let max = self.config.limits.max_output_mb * 1024 * 1024;
        let execution_started = Instant::now();
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
            if bytes>max{lease.engine.record_phase(&lease.id, |p| p.execution_ms = execution_started.elapsed().as_secs_f64() * 1000.);lease.finish("failed",Some("output budget exceeded".into()));Err(df_error("output budget exceeded"))?;}
            if let Some(e)=lease.engine.history.lock().unwrap().get_mut(&lease.id){e.diagnostic.rows+=batch.num_rows() as u64;e.diagnostic.arrow_bytes=bytes as u64;}
            yield batch;
           },Some(Err(e))=>{lease.engine.record_phase(&lease.id, |p| p.execution_ms = execution_started.elapsed().as_secs_f64() * 1000.);lease.finish("failed",Some(e.to_string()));Err(e)?;},None=>{
             lease.engine.record_phase(&lease.id, |p| p.execution_ms = execution_started.elapsed().as_secs_f64() * 1000.);
             if let Some(e)=lease.engine.history.lock().unwrap().get_mut(&lease.id){e.diagnostic.physical_plan=Some(format!("{}",datafusion::physical_plan::display::DisplayableExecutionPlan::with_metrics(physical.as_ref()).indent(true)));}
             // LIMIT and other early-finish operators can retain an unfinished
             // source stream until their enclosing execution stream is dropped.
             drop(input);
             // End the source snapshots only after every batch was consumed. A
             // failed cleanup, cancellation or timeout discards the connection.
             // Do this before Lease::finish cancels the shared query token.
             let cleanup_started = Instant::now();
             let cleanup = tokio::select! {
                 biased;
                 _ = cancel.cancelled() => Err(anyhow::anyhow!("query cancelled")),
                 result = tokio::time::timeout_at(deadline, async {
                     for session in postgres_sessions {
                         session.finish().await?;
                     }
                     Ok::<_, anyhow::Error>(())
                 }) => result.unwrap_or_else(|_| Err(anyhow::anyhow!("query deadline exceeded during source cleanup"))),
             };
             lease.engine.record_phase(&lease.id, |p| p.cleanup_ms = cleanup_started.elapsed().as_secs_f64() * 1000.);
             if let Err(error) = cleanup {
                 lease.finish("failed", Some(error.to_string()));
                 Err(df_error(error))?;
             }
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
        let selected_partitions = self.admitted_partitions();
        let _guard = token.clone().drop_guard();
        tokio::time::timeout(
            Duration::from_secs(self.config.limits.query_timeout_secs),
            async {
                let (ctx, postgres_sessions) = self
                    .context(
                        sql,
                        Metrics::default(),
                        token,
                        "describe",
                        selected_partitions,
                    )
                    .await?;
                let df = ctx.sql(sql).await?;
                let schema = Arc::new(df.schema().as_arrow().clone());
                for session in postgres_sessions {
                    session.finish().await?;
                }
                Ok::<_, anyhow::Error>(schema)
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
        let diagnostic = e.diagnostic(&id).unwrap();
        assert_eq!(diagnostic.status, "succeeded");
        let phases = diagnostic.phases;
        let total = phases.catalog_ms
            + phases.logical_plan_ms
            + phases.optimize_ms
            + phases.physical_plan_ms
            + phases.execution_ms
            + phases.cleanup_ms;
        assert!(total > 0. && total <= diagnostic.elapsed_ms);
        assert!(diagnostic.logical_plan.unwrap().contains("Int64(3)"));
        assert!(diagnostic.physical_plan.is_some());
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

    #[test]
    fn table_selection_understands_ctes_catalogs_and_quoted_identifiers() {
        let selection = TableSelection::parse(
            "WITH lake AS (SELECT * FROM datafusion.PG.Customers), \
             nested AS (SELECT * FROM lake) \
             SELECT * FROM nested JOIN \"Lake\".\"Orders\" ON true \
             WHERE EXISTS (SELECT 1 FROM pg.forbidden)",
        )
        .unwrap();
        assert!(selection.includes("pg", "customers"));
        assert!(selection.includes("pg", "forbidden"));
        assert!(selection.includes("Lake", "Orders"));
        assert!(!selection.includes("lake", "orders"));
        assert!(!selection.includes("pg", "unreferenced"));
        let external = TableSelection::parse("SELECT * FROM other.pg.customers").unwrap();
        assert!(!external.includes("pg", "customers"));
        let introspection =
            TableSelection::parse("SELECT * FROM datafusion.information_schema.tables").unwrap();
        assert!(introspection.includes("pg", "unreferenced"));
    }

    #[test]
    fn cache_capacity_is_carved_out_of_the_shared_execution_budget() {
        use datafusion::execution::memory_pool::MemoryConsumer;
        let mut config: Config = toml::from_str("").unwrap();
        config.limits.memory_mb = 64;
        config.limits.file_cache_mb = 24;
        let engine = Engine::new(config).unwrap();
        let allocation = MemoryConsumer::new("test concurrent execution").register(&engine.pool);
        allocation.try_grow(40 * 1024 * 1024).unwrap();
        let other_query = MemoryConsumer::new("second concurrent execution").register(&engine.pool);
        assert!(other_query.try_grow(1).is_err());
        drop(allocation);
        other_query.try_grow(40 * 1024 * 1024).unwrap();
        drop(other_query);
        assert_eq!(engine.pool.reserved(), 0);
    }

    #[tokio::test]
    async fn admission_and_cancellation_are_isolated_between_queries() {
        let mut config: Config = toml::from_str("").unwrap();
        config.limits.max_concurrent_queries = 2;
        let engine = Engine::new(config).unwrap();
        let mut first = engine.query("SELECT 1").await.unwrap();
        let mut second = engine.query("SELECT 2").await.unwrap();
        assert!(engine.query("SELECT 3").await.is_err());
        assert!(engine.cancel(&first.id));
        assert!(first.stream.next().await.unwrap().is_err());
        drop(first);
        assert!(second.stream.next().await.unwrap().is_ok());
        assert!(second.stream.next().await.is_none());
        let (id, batches) = engine.collect("SELECT 3").await.unwrap();
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(engine.diagnostic(&id).unwrap().status, "succeeded");
        assert_eq!(engine.pool.reserved(), 0);
    }

    #[tokio::test]
    async fn partition_targets_follow_admission_without_mutating_running_plans() {
        let mut config: Config = toml::from_str("").unwrap();
        config.limits.partitions = 2;
        config.limits.max_concurrent_queries = 3;
        config.limits.adaptive_partitions = true;
        let engine = Engine::new(config).unwrap();
        let mut first = engine.query("SELECT 1").await.unwrap();
        let second = engine.query("SELECT 2").await.unwrap();
        let third = engine.query("SELECT 3").await.unwrap();
        assert_eq!(engine.diagnostic(&first.id).unwrap().selected_partitions, 2);
        assert_eq!(
            engine.diagnostic(&second.id).unwrap().selected_partitions,
            1
        );
        assert_eq!(engine.diagnostic(&third.id).unwrap().selected_partitions, 1);
        assert!(engine.query("SELECT 4").await.is_err());
        assert!(engine.cancel(&first.id));
        assert!(first.stream.next().await.unwrap().is_err());
        drop(first);
        // Targets belong to immutable plans, even when an earlier query exits.
        assert_eq!(
            engine.diagnostic(&second.id).unwrap().selected_partitions,
            1
        );
        drop(second);
        drop(third);
        let (id, _) = engine.collect("SELECT 4").await.unwrap();
        assert_eq!(engine.diagnostic(&id).unwrap().selected_partitions, 2);
        assert_eq!(engine.pool.reserved(), 0);
    }

    #[tokio::test]
    async fn adaptive_partitioning_can_be_disabled() {
        let config: Config = toml::from_str(
            "[limits]\npartitions=2\nmax_concurrent_queries=2\nadaptive_partitions=false",
        )
        .unwrap();
        let engine = Engine::new(config).unwrap();
        let first = engine.query("SELECT 1").await.unwrap();
        let second = engine.query("SELECT 2").await.unwrap();
        assert_eq!(engine.diagnostic(&first.id).unwrap().selected_partitions, 2);
        assert_eq!(
            engine.diagnostic(&second.id).unwrap().selected_partitions,
            2
        );
    }

    #[tokio::test]
    async fn unreferenced_parquet_is_not_opened_and_contexts_stay_isolated() {
        use datafusion::{
            arrow::{
                array::Int64Array,
                datatypes::{DataType, Field, Schema},
            },
            parquet::arrow::ArrowWriter,
        };
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("rows.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let mut writer =
            ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let second_path = directory.path().join("more-rows.parquet");
        std::fs::copy(&path, &second_path).unwrap();
        let mut config: Config = toml::from_str("").unwrap();
        config.limits.partitions = 2;
        config.limits.max_concurrent_queries = 2;
        config.limits.adaptive_partitions = true;
        let table = |name: &str, path: String| crate::config::Table {
            name: name.into(),
            remote: None,
            columns: vec![],
            files: vec![crate::config::FilePin { path, etag: None }],
            metadata_location: None,
            snapshot_id: None,
            snapshot: None,
        };
        config.sources.push(crate::config::Source {
            name: "lake".into(),
            connection: Connection::Parquet {},
            tables: vec![
                table("available", path.to_string_lossy().into_owned()),
                table(
                    "unavailable",
                    directory
                        .path()
                        .join("missing.parquet")
                        .to_string_lossy()
                        .into_owned(),
                ),
            ],
        });
        let mut parallel = table("parallel", path.to_string_lossy().into_owned());
        parallel.files.push(crate::config::FilePin {
            path: second_path.to_string_lossy().into_owned(),
            etag: None,
        });
        config.sources[0].tables.push(parallel);
        let engine = Engine::new(config).unwrap();
        let sql = "WITH x AS (SELECT n FROM datafusion.lake.available) SELECT SUM(n) FROM x";
        let (a, b) = tokio::join!(engine.collect(sql), engine.collect(sql));
        for result in [a, b] {
            let (_, batches) = result.unwrap();
            assert_eq!(
                crate::api::row(&batches[0], 0).unwrap(),
                vec![Some("6".into())]
            );
        }
        let first = engine
            .query("SELECT SUM(n) FROM lake.parallel")
            .await
            .unwrap();
        let second = engine
            .query("SELECT SUM(n) FROM lake.parallel")
            .await
            .unwrap();
        assert!(
            engine
                .diagnostic(&first.id)
                .unwrap()
                .physical_plan
                .unwrap()
                .contains("file_groups={2 groups")
        );
        assert!(
            engine
                .diagnostic(&second.id)
                .unwrap()
                .physical_plan
                .unwrap()
                .contains("file_groups={1 group")
        );
        for mut query in [first, second] {
            let batch = query.stream.next().await.unwrap().unwrap();
            assert_eq!(crate::api::row(&batch, 0).unwrap(), vec![Some("12".into())]);
            assert!(query.stream.next().await.is_none());
        }
        assert!(
            engine
                .collect("SELECT * FROM lake.unavailable")
                .await
                .is_err()
        );
        // A failed catalog initialization must not contaminate later contexts.
        assert!(engine.collect(sql).await.is_ok());
        // Introspection intentionally resolves every configured table, so it
        // must still report the inaccessible configured file as an error.
        assert!(
            engine
                .collect("SELECT * FROM information_schema.tables")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn explain_and_query_time_remain_valid_after_single_optimization() {
        let engine = engine();
        let (_, batches) = engine
            .collect(
                "WITH clock AS (SELECT now() AS t) SELECT t = now() AS same_snapshot FROM clock",
            )
            .await
            .unwrap();
        assert_eq!(
            crate::api::row(&batches[0], 0).unwrap(),
            vec![Some("true".into())]
        );
        let (_, explain) = engine.collect("EXPLAIN SELECT 1 + 2 AS n").await.unwrap();
        assert!(explain.iter().map(RecordBatch::num_rows).sum::<usize>() >= 2);
    }
}
