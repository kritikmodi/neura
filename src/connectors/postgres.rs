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
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_postgres::{Client, SimpleQueryMessage};
use tokio_util::sync::CancellationToken;
pub struct Session {
    pub client: Mutex<Client>,
}
impl Session {
    pub async fn connect(
        env: &str,
        allow_insecure: bool,
        limits: &Limits,
        cancel: CancellationToken,
    ) -> Result<Arc<Self>> {
        let dsn = secret(env)?;
        let config: tokio_postgres::Config = dsn
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid PostgreSQL DSN in {env}"))?;
        ensure!(
            allow_insecure || config.get_ssl_mode() == tokio_postgres::config::SslMode::Require,
            "PostgreSQL requires sslmode=require (verified native TLS), or explicit allow_insecure for local tests"
        );
        let tls = postgres_native_tls::MakeTlsConnector::new(native_tls::TlsConnector::new()?);
        let (client, conn) = config.connect(tls.clone()).await.map_err(|_| {
            anyhow::anyhow!(
                "PostgreSQL connection failed for {env}; check reachability, TLS and credentials"
            )
        })?;
        let ct = client.cancel_token();
        tokio::spawn(async move {
            tokio::select! {_ = conn=>{},_ = cancel.cancelled()=>{let _=ct.cancel_query(tls).await;}}
        });
        client.batch_execute(&format!("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET LOCAL statement_timeout = {}; SET LOCAL idle_in_transaction_session_timeout = {}; SET LOCAL timezone='UTC'; SET LOCAL datestyle='ISO, YMD'; SET LOCAL extra_float_digits=3",limits.query_timeout_secs*1000,limits.query_timeout_secs*1000)).await.context("unable to start read-only PostgreSQL snapshot")?;
        let role = client
            .query_one(
                "SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname = current_user",
                &[],
            )
            .await?;
        ensure!(
            !role.get::<_, bool>(0) && !role.get::<_, bool>(1),
            "PostgreSQL superuser/BYPASSRLS credentials are forbidden"
        );
        Ok(Arc::new(Self {
            client: Mutex::new(client),
        }))
    }
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
     let client=pg.client.lock().await;
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
