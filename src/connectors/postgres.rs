use super::df_error;
use crate::{
    arrow_util,
    config::{Limits, secret},
    metrics::Metrics,
};
use anyhow::{Context, Result, ensure};
use datafusion::{
    arrow::datatypes::SchemaRef,
    error::Result as DfResult,
    physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter},
};
use futures::StreamExt;
use sha2::{Digest, Sha256};
use std::{
    ops::Deref,
    sync::{Arc, Mutex as StdMutex},
    time::{Duration, Instant},
};
use tokio::{
    sync::{Mutex, MutexGuard, OwnedSemaphorePermit, Semaphore},
    task::JoinHandle,
};
use tokio_postgres::{Client, SimpleQueryMessage};
use tokio_util::sync::CancellationToken;
// A short lifetime bounds reuse of an authenticated backend after an external
// credential change. The role and all table privileges are checked on every
// query; database snapshots are never cached or shared between queries.
const CONNECTION_LIFETIME: Duration = Duration::from_secs(60);
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(2);

pub struct Pool {
    slots: Arc<Semaphore>,
    idle: StdMutex<Vec<Connection>>,
}
impl Pool {
    pub fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            slots: Arc::new(Semaphore::new(capacity)),
            idle: StdMutex::new(Vec::with_capacity(capacity)),
        })
    }
    fn take(&self, scope: &[u8; 32]) -> Option<Connection> {
        let mut idle = self.idle.lock().unwrap();
        // Re-resolve the environment secret on every checkout. Connections
        // established under a previous DSN, credential, or TLS mode are closed.
        idle.retain(|c| c.reusable(scope));
        idle.pop()
    }
}

struct Connection {
    client: Client,
    driver: JoinHandle<()>,
    tls: postgres_native_tls::MakeTlsConnector,
    scope: [u8; 32],
    opened: Instant,
}
impl Connection {
    fn reusable(&self, scope: &[u8; 32]) -> bool {
        self.scope == *scope
            && !self.client.is_closed()
            && !self.driver.is_finished()
            && self.opened.elapsed() < CONNECTION_LIFETIME
    }
}
impl Drop for Connection {
    fn drop(&mut self) {
        // Dropping a driver closes its socket. Never let a failed/abandoned
        // transaction become an idle connection in the pool.
        self.driver.abort();
    }
}

pub struct Session {
    connection: Mutex<Option<Connection>>,
    watch: StdMutex<Option<JoinHandle<()>>>,
    cancel: CancellationToken,
    pool: Option<Arc<Pool>>,
    permit: StdMutex<Option<OwnedSemaphorePermit>>,
}
pub struct ClientGuard<'a>(MutexGuard<'a, Option<Connection>>);
impl Deref for ClientGuard<'_> {
    type Target = Client;
    fn deref(&self) -> &Self::Target {
        &self.0.as_ref().expect("checked PostgreSQL session").client
    }
}
impl Drop for Session {
    fn drop(&mut self) {
        // The already-running watcher sends a bounded CancelRequest even when
        // the consumer abandons a running stream. It holds no pool permit or
        // Session reference and cannot retain an idle transaction indefinitely.
        if self.connection.get_mut().is_some() {
            self.cancel.cancel();
        }
    }
}
impl Session {
    pub async fn connect(
        env: &str,
        allow_insecure: bool,
        limits: &Limits,
        cancel: CancellationToken,
    ) -> Result<Arc<Self>> {
        Self::connect_inner(None, env, allow_insecure, limits, cancel).await
    }

    pub async fn connect_pooled(
        pool: Arc<Pool>,
        env: &str,
        allow_insecure: bool,
        limits: &Limits,
        cancel: CancellationToken,
    ) -> Result<Arc<Self>> {
        Self::connect_inner(Some(pool), env, allow_insecure, limits, cancel).await
    }

    async fn connect_inner(
        pool: Option<Arc<Pool>>,
        env: &str,
        allow_insecure: bool,
        limits: &Limits,
        cancel: CancellationToken,
    ) -> Result<Arc<Self>> {
        let dsn = secret(env)?;
        Self::connect_dsn(pool, env, &dsn, allow_insecure, limits, cancel).await
    }

    async fn connect_dsn(
        pool: Option<Arc<Pool>>,
        env: &str,
        dsn: &str,
        allow_insecure: bool,
        limits: &Limits,
        cancel: CancellationToken,
    ) -> Result<Arc<Self>> {
        let config: tokio_postgres::Config = dsn
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid PostgreSQL DSN in {env}"))?;
        ensure!(
            allow_insecure || config.get_ssl_mode() == tokio_postgres::config::SslMode::Require,
            "PostgreSQL requires sslmode=require (verified native TLS), or explicit allow_insecure for local tests"
        );
        let scope = connection_scope(env, dsn, allow_insecure);
        let permit = if let Some(pool) = &pool {
            Some(tokio::select! {
                _ = cancel.cancelled() => anyhow::bail!("query cancelled"),
                p = pool.slots.clone().acquire_owned() => p.context("PostgreSQL pool closed")?,
            })
        } else {
            None
        };
        let connection = match pool.as_ref().and_then(|p| p.take(&scope)) {
            Some(c) => c,
            None => {
                let tls =
                    postgres_native_tls::MakeTlsConnector::new(native_tls::TlsConnector::new()?);
                let (client, conn) = tokio::select! {
                    _ = cancel.cancelled() => anyhow::bail!("query cancelled"),
                    result = config.connect(tls.clone()) => result.map_err(|_| anyhow::anyhow!(
                        "PostgreSQL connection failed for {env}; check reachability, TLS and credentials"
                    ))?,
                };
                Connection {
                    client,
                    driver: tokio::spawn(async move {
                        let _ = conn.await;
                    }),
                    tls,
                    scope,
                    opened: Instant::now(),
                }
            }
        };
        let ct = connection.client.cancel_token();
        let tls = connection.tls.clone();
        let driver = connection.driver.abort_handle();
        let query_cancel = cancel.clone();
        let watch = tokio::spawn(async move {
            query_cancel.cancelled().await;
            let _ = tokio::time::timeout(CLEANUP_TIMEOUT, ct.cancel_query(tls)).await;
            driver.abort();
        });
        let session = Arc::new(Self {
            connection: Mutex::new(Some(connection)),
            watch: StdMutex::new(Some(watch)),
            cancel,
            pool,
            permit: StdMutex::new(permit),
        });
        // Use one protocol round trip for setup and the mandatory live role
        // check. Its SELECT also establishes this query's repeatable-read
        // snapshot before planning/execution can observe the source.
        let timeout_ms = limits.query_timeout_secs.saturating_mul(1000);
        let setup = format!(
            "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET LOCAL statement_timeout = {timeout_ms}; SET LOCAL idle_in_transaction_session_timeout = {timeout_ms}; SET LOCAL timezone='UTC'; SET LOCAL datestyle='ISO, YMD'; SET LOCAL extra_float_digits=3; SELECT rolsuper, rolbypassrls, rolcanlogin, current_user = session_user FROM pg_roles WHERE rolname = current_user"
        );
        let role = {
            let client = session.client().await?;
            tokio::select! {
                _ = session.cancel.cancelled() => anyhow::bail!("query cancelled"),
                result = client.simple_query(&setup) => result.context("unable to start read-only PostgreSQL snapshot")?,
            }
        };
        let role = role
            .iter()
            .find_map(|m| match m {
                SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .context("PostgreSQL current role is unavailable")?;
        ensure!(
            role.get(0) == Some("f") && role.get(1) == Some("f"),
            "PostgreSQL superuser/BYPASSRLS credentials are forbidden",
        );
        ensure!(
            role.get(2) == Some("t") && role.get(3) == Some("t"),
            "PostgreSQL login is disabled or session identity changed",
        );
        Ok(session)
    }

    pub async fn client(&self) -> Result<ClientGuard<'_>> {
        let connection = self.connection.lock().await;
        ensure!(
            connection.is_some(),
            "PostgreSQL session has already finished"
        );
        Ok(ClientGuard(connection))
    }

    /// Call only after the complete query stream has been consumed successfully,
    /// before cancelling its query token. All other exits discard the socket.
    pub async fn finish(&self) -> Result<()> {
        let mut connection = self.connection.lock().await;
        let Some(conn) = connection.take() else {
            return Ok(());
        };
        let watch = self.watch.lock().unwrap().take();
        if let Some(watch) = watch {
            watch.abort();
            // Joining guarantees an old cancellation watcher cannot later send
            // CancelRequest for this backend while a new query is using it.
            let _ = watch.await;
        }
        if self.cancel.is_cancelled() {
            anyhow::bail!("query cancelled");
        }
        // DISCARD ALL removes any session state modified by source functions.
        // It must follow ROLLBACK, outside the repeatable-read transaction.
        // Separate protocol messages are required: putting DISCARD ALL in the
        // same simple-query batch creates an implicit transaction block.
        tokio::time::timeout(CLEANUP_TIMEOUT, async {
            conn.client.batch_execute("ROLLBACK").await?;
            conn.client.batch_execute("DISCARD ALL").await
        })
        .await
        .context("PostgreSQL connection cleanup timed out")?
        .context("PostgreSQL connection cleanup failed")?;
        if !self.cancel.is_cancelled()
            && conn.reusable(&conn.scope)
            && let Some(pool) = &self.pool
        {
            pool.idle.lock().unwrap().push(conn);
        }
        self.permit.lock().unwrap().take();
        Ok(())
    }
}

fn connection_scope(env: &str, dsn: &str, allow_insecure: bool) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(env.as_bytes());
    hash.update([0]);
    hash.update(dsn.as_bytes());
    hash.update([u8::from(allow_insecure)]);
    hash.finalize().into()
}
pub fn execute(
    pg: Arc<Session>,
    query: String,
    schema: SchemaRef,
    metrics: Metrics,
    name: String,
    limits: Limits,
    cancel: CancellationToken,
) -> DfResult<SendableRecordBatchStream> {
    let out = schema.clone();
    let stream = async_stream::try_stream! {
     let _timer=metrics.timer(&name);
     let client=pg.client().await.map_err(df_error)?;
     let rows=client.simple_query_raw(&query).await.map_err(|_|df_error("PostgreSQL query failed; inspect source logs"))?;
     futures::pin_mut!(rows);
     let mut buffer=Vec::with_capacity(limits.batch_rows);let mut size=0usize;
     loop {
      let next=tokio::select!{_ = cancel.cancelled()=>Err(df_error("query cancelled")),r=rows.next()=>Ok(r)}?;
      let Some(row)=next else{break;};
      match row.map_err(|_|df_error("PostgreSQL stream failed; inspect source logs"))?{
       SimpleQueryMessage::Row(r)=>{
        let row:Vec<Option<String>>=(0..r.len()).map(|i|r.get(i).map(str::to_string)).collect();
        let n=row.iter().flatten().map(String::len).sum::<usize>();size+=n;
        if size>limits.max_remote_page_mb*1024*1024 {Err(df_error("PostgreSQL batch exceeds remote buffer limit"))?;}
        metrics.update(&name,|m|{m.rows+=1;m.response_bytes=Some(m.response_bytes.unwrap_or(0)+n as u64);});buffer.push(row);
        if buffer.len()>=limits.batch_rows {let b=arrow_util::batch(schema.clone(),&buffer).map_err(df_error)?;metrics.update(&name,|m|m.arrow_bytes+=b.get_array_memory_size() as u64);buffer.clear();size=0;yield b;}
       },_=>{}
      }
     }
     if !buffer.is_empty(){let b=arrow_util::batch(schema.clone(),&buffer).map_err(df_error)?;metrics.update(&name,|m|m.arrow_bytes+=b.get_array_memory_size() as u64);yield b;}
    };
    Ok(Box::pin(RecordBatchStreamAdapter::new(out, stream)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_scope_separates_credentials_and_tls() {
        let scope = connection_scope("SOURCE_A", "host=db user=a password=one", false);
        assert_ne!(
            scope,
            connection_scope("SOURCE_B", "host=db user=a password=one", false)
        );
        assert_ne!(
            scope,
            connection_scope("SOURCE_A", "host=db user=a password=two", false)
        );
        assert_ne!(
            scope,
            connection_scope("SOURCE_A", "host=db user=a password=one", true)
        );
    }

    async fn admin() -> Client {
        let (client, driver) = tokio_postgres::connect(
            "host=127.0.0.1 port=55432 user=postgres password=neura-test-admin-only dbname=neura",
            tokio_postgres::NoTls,
        )
        .await
        .expect("requires the disposable PostgreSQL fixture on port 55432");
        tokio::spawn(async move {
            let _ = driver.await;
        });
        client
    }

    async fn checkout(
        pool: &Arc<Pool>,
        dsn: &str,
        cancel: CancellationToken,
    ) -> Result<Arc<Session>> {
        Session::connect_dsn(
            Some(pool.clone()),
            "pool_test",
            dsn,
            true,
            &Limits::default(),
            cancel,
        )
        .await
    }

    async fn number(session: &Session, sql: &str) -> i64 {
        session
            .client()
            .await
            .unwrap()
            .query_one(sql, &[])
            .await
            .unwrap()
            .get(0)
    }

    #[tokio::test]
    #[ignore = "requires real disposable PostgreSQL fixture; no cloud credentials"]
    async fn postgres_pool_reuses_socket_but_refreshes_snapshot_permissions_and_identity() {
        let admin = admin().await;
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let role = format!("neura_pool_{suffix}");
        let table = format!("neura_pool_table_{suffix}");
        let dsn = format!(
            "host=127.0.0.1 port=55432 user={role} password={suffix} dbname=neura sslmode=disable"
        );
        admin.batch_execute(&format!(
            "CREATE ROLE {role} LOGIN PASSWORD '{suffix}'; CREATE TABLE {table}(id bigint, tenant bigint, value bigint); INSERT INTO {table} VALUES (1,1,10),(2,2,99); ALTER TABLE {table} ENABLE ROW LEVEL SECURITY; CREATE POLICY tenant ON {table} USING (tenant=1); GRANT SELECT ON {table} TO {role}"
        )).await.unwrap();
        let pool = Pool::new(1);
        let first = checkout(&pool, &dsn, CancellationToken::new())
            .await
            .unwrap();
        let pid = number(&first, "SELECT pg_backend_pid()::bigint").await;
        assert_eq!(
            number(&first, &format!("SELECT COUNT(*) FROM {table}")).await,
            1
        );
        assert_eq!(
            number(&first, &format!("SELECT value FROM {table} WHERE id=1")).await,
            10
        );
        admin
            .batch_execute(&format!("UPDATE {table} SET value=20 WHERE id=1"))
            .await
            .unwrap();
        assert_eq!(
            number(&first, &format!("SELECT value FROM {table} WHERE id=1")).await,
            10
        );
        first
            .client()
            .await
            .unwrap()
            .batch_execute(
                "SET SESSION application_name='tainted'; SET SESSION timezone='Pacific/Auckland'",
            )
            .await
            .unwrap();
        first.finish().await.unwrap();
        let state = admin
            .query_one(
                "SELECT state, xact_start IS NULL FROM pg_stat_activity WHERE pid=$1",
                &[&(pid as i32)],
            )
            .await
            .unwrap();
        assert_eq!(state.get::<_, &str>(0), "idle");
        assert!(
            state.get::<_, bool>(1),
            "pooled backend must not retain a transaction"
        );
        let second = checkout(&pool, &dsn, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            number(&second, "SELECT pg_backend_pid()::bigint").await,
            pid
        );
        assert_eq!(
            number(&second, &format!("SELECT value FROM {table} WHERE id=1")).await,
            20
        );
        let settings = second.client().await.unwrap().query_one("SELECT current_setting('application_name'), current_setting('timezone'), current_setting('transaction_read_only'), current_setting('transaction_isolation')", &[]).await.unwrap();
        assert_ne!(settings.get::<_, &str>(0), "tainted");
        assert_eq!(settings.get::<_, &str>(1), "UTC");
        assert_eq!(settings.get::<_, &str>(2), "on");
        assert_eq!(settings.get::<_, &str>(3), "repeatable read");
        second.finish().await.unwrap();

        // Privileges must still be checked by PostgreSQL on a reused backend.
        admin
            .batch_execute(&format!("REVOKE SELECT ON {table} FROM {role}"))
            .await
            .unwrap();
        let denied = checkout(&pool, &dsn, CancellationToken::new())
            .await
            .unwrap();
        let error = denied
            .client()
            .await
            .unwrap()
            .simple_query(&format!("SELECT * FROM {table}"))
            .await
            .unwrap_err();
        assert_eq!(
            error.code(),
            Some(&tokio_postgres::error::SqlState::INSUFFICIENT_PRIVILEGE)
        );
        drop(denied);
        assert!(pool.idle.lock().unwrap().is_empty());

        // Each checkout rechecks source role flags, including on an existing
        // authenticated socket, so elevated or NOLOGIN credentials cannot run.
        let clean = checkout(&pool, &dsn, CancellationToken::new())
            .await
            .unwrap();
        clean.finish().await.unwrap();
        admin
            .batch_execute(&format!("ALTER ROLE {role} BYPASSRLS"))
            .await
            .unwrap();
        let error = checkout(&pool, &dsn, CancellationToken::new())
            .await
            .err()
            .expect("BYPASSRLS must be rejected");
        assert!(error.to_string().contains("BYPASSRLS"));
        admin
            .batch_execute(&format!("ALTER ROLE {role} NOBYPASSRLS"))
            .await
            .unwrap();
        let clean = checkout(&pool, &dsn, CancellationToken::new())
            .await
            .unwrap();
        clean.finish().await.unwrap();
        admin
            .batch_execute(&format!("ALTER ROLE {role} NOLOGIN"))
            .await
            .unwrap();
        let error = checkout(&pool, &dsn, CancellationToken::new())
            .await
            .err()
            .expect("NOLOGIN must be rejected on pooled connections");
        assert!(error.to_string().contains("login is disabled"));
        assert!(pool.idle.lock().unwrap().is_empty());
        drop(pool);
        admin
            .batch_execute(&format!("DROP TABLE {table}; DROP ROLE {role}"))
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires real disposable PostgreSQL fixture; no cloud credentials"]
    async fn postgres_pool_bounds_checkout_and_discards_cancelled_or_dropped_sessions() {
        let admin = admin().await;
        let dsn = "host=127.0.0.1 port=55432 user=neura_reader password=neura-test-reader-only dbname=neura sslmode=disable";
        let pool = Pool::new(1);
        let first = checkout(&pool, dsn, CancellationToken::new())
            .await
            .unwrap();
        let first_pid = number(&first, "SELECT pg_backend_pid()::bigint").await;
        assert!(
            tokio::time::timeout(
                Duration::from_millis(50),
                checkout(&pool, dsn, CancellationToken::new())
            )
            .await
            .is_err()
        );
        let waiting_cancel = CancellationToken::new();
        waiting_cancel.cancel();
        assert!(checkout(&pool, dsn, waiting_cancel).await.is_err());
        drop(first);
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if admin
                    .query_one(
                        "SELECT COUNT(*) FROM pg_stat_activity WHERE pid=$1",
                        &[&(first_pid as i32)],
                    )
                    .await
                    .unwrap()
                    .get::<_, i64>(0)
                    == 0
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("dropped session left its backend alive");
        let cancel = CancellationToken::new();
        let session = checkout(&pool, dsn, cancel.clone()).await.unwrap();
        let pid = number(&session, "SELECT pg_backend_pid()::bigint").await;
        assert_ne!(first_pid, pid);
        let running = session.clone();
        let statement = tokio::spawn(async move {
            running
                .client()
                .await
                .unwrap()
                .simple_query("SELECT pg_sleep(30)")
                .await
        });
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if admin.query_one("SELECT COUNT(*) FROM pg_stat_activity WHERE pid=$1 AND wait_event='PgSleep'", &[&(pid as i32)]).await.unwrap().get::<_, i64>(0) == 1 { break; }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("sleep statement never started");
        cancel.cancel();
        assert!(
            tokio::time::timeout(Duration::from_secs(3), statement)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
        drop(session);
        let next = checkout(&pool, dsn, CancellationToken::new())
            .await
            .unwrap();
        assert_ne!(number(&next, "SELECT pg_backend_pid()::bigint").await, pid);
        assert_eq!(number(&next, "SELECT 1::bigint").await, 1);
        next.finish().await.unwrap();
    }
}
