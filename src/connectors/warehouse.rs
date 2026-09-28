//! Governed vendor SQL endpoints; no direct access to warehouse backing files.
use super::{Remote, df_error};
use crate::{
    arrow_util,
    config::{Connection, secret},
    metrics::Metrics,
};
use anyhow::{Context, Result, bail, ensure};
use datafusion::{
    arrow::datatypes::{DataType, SchemaRef},
    physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter},
};
use futures::StreamExt;
use reqwest::{Client, Method};
use serde_json::{Value, json};
use std::time::Duration;

#[derive(Clone)]
pub struct Http {
    client: Client,
    max_bytes: usize,
}
impl Http {
    pub fn new(max_mb: usize) -> Result<Self> {
        Ok(Self {
            client: Client::builder()
                .https_only(true)
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(60))
                .user_agent("neura/0.1")
                .build()?,
            max_bytes: max_mb * 1024 * 1024,
        })
    }
    async fn request(
        &self,
        method: Method,
        url: &str,
        token: &str,
        body: Option<Value>,
        metrics: &Metrics,
        name: &str,
    ) -> Result<Value> {
        let _timer = metrics.timer(name);
        let mut req = self.client.request(method, url).bearer_auth(token);
        if let Some(body) = body {
            req = req.json(&body);
        }
        let response = req
            .send()
            .await
            .map_err(|_| anyhow::anyhow!("{name} API transport failed"))?;
        ensure!(
            response.status().is_success(),
            "{name} API returned HTTP {}; consult vendor query history",
            response.status().as_u16()
        );
        ensure!(
            response.content_length().unwrap_or(0) <= self.max_bytes as u64,
            "{name} API page exceeds configured buffer limit"
        );
        let mut stream = response.bytes_stream();
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            let b = chunk.map_err(|_| anyhow::anyhow!("{name} response interrupted"))?;
            ensure!(
                bytes.len() + b.len() <= self.max_bytes,
                "{name} API page exceeds configured buffer limit"
            );
            bytes.extend_from_slice(&b);
            metrics.update(name, |m| {
                m.response_bytes = Some(m.response_bytes.unwrap_or(0) + b.len() as u64)
            });
        }
        if bytes.is_empty() {
            return Ok(json!({}));
        }
        serde_json::from_slice(&bytes).context("invalid vendor JSON response")
    }
}
struct RemoteJob {
    http: Http,
    url: String,
    token: String,
    metrics: Metrics,
    name: String,
    finished: bool,
}
impl Drop for RemoteJob {
    fn drop(&mut self) {
        if !self.finished {
            let http = self.http.clone();
            let url = self.url.clone();
            let token = self.token.clone();
            let metrics = self.metrics.clone();
            let name = self.name.clone();
            tokio::spawn(async move {
                let _ = http
                    .request(Method::POST, &url, &token, Some(json!({})), &metrics, &name)
                    .await;
            });
        }
    }
}
fn string(v: &Value, key: &str) -> Result<String> {
    v[key]
        .as_str()
        .map(str::to_owned)
        .with_context(|| format!("vendor response missing {key}"))
}
fn safe_id(id: &str) -> Result<()> {
    ensure!(
        !id.is_empty()
            && id
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c)),
        "invalid vendor job identifier"
    );
    Ok(())
}
fn rows(v: &Value, bq: bool) -> Result<Vec<Vec<Option<String>>>> {
    let Some(items) = v.as_array() else {
        if v.is_null() {
            return Ok(vec![]);
        }
        bail!("invalid vendor rows");
    };
    items
        .iter()
        .map(|r| {
            let cells =
                if bq { r["f"].as_array() } else { r.as_array() }.context("invalid vendor row")?;
            cells
                .iter()
                .map(|c| {
                    let c = if bq { &c["v"] } else { c };
                    match c {
                        Value::Null => Ok(None),
                        Value::String(s) => Ok(Some(s.clone())),
                        Value::Bool(b) => Ok(Some(b.to_string())),
                        Value::Number(n) => Ok(Some(n.to_string())),
                        _ => bail!("nested vendor values unsupported; use a source view"),
                    }
                })
                .collect()
        })
        .collect()
}
fn normalize_bq(rows: &mut [Vec<Option<String>>], schema: &SchemaRef) -> Result<()> {
    for r in rows {
        for (v, f) in r.iter_mut().zip(schema.fields()) {
            if matches!(f.data_type(), DataType::Timestamp(_, Some(_)))
                && let Some(s) = v
            {
                // Decimal seconds to microseconds without passing through float64.
                let negative = s.starts_with('-');
                let abs = s.trim_start_matches('-');
                let (secs, frac) = abs.split_once('.').unwrap_or((abs, ""));
                ensure!(
                    frac.len() <= 6 || frac[6..].bytes().all(|c| c == b'0'),
                    "timestamp precision exceeds microseconds"
                );
                let nanos = format!("{:0<6}", &frac[..frac.len().min(6)]).parse::<i64>()?;
                let mut micros = secs
                    .parse::<i64>()?
                    .checked_mul(1_000_000)
                    .and_then(|v| v.checked_add(nanos))
                    .context("timestamp overflow")?;
                if negative {
                    micros = -micros;
                }
                *s = chrono::DateTime::from_timestamp_micros(micros)
                    .context("timestamp out of range")?
                    .to_rfc3339();
            }
        }
    }
    Ok(())
}
pub fn execute(
    remote: Remote,
    query: String,
    schema: SchemaRef,
) -> datafusion::error::Result<SendableRecordBatchStream> {
    let out = schema.clone();
    let stream = async_stream::try_stream! {
     let result:Result<_>=async {
      let name=&remote.source.name;let http=remote.http.as_ref().ok_or_else(|| df_error("warehouse client missing"))?;let metrics=&remote.metrics;
      let (base,token,body,url)=match &remote.source.connection{
       Connection::Databricks{host,warehouse_id,token_env}=>{let b=host.trim_end_matches('/').to_string();(b.clone(),secret(token_env)?,json!({"warehouse_id":warehouse_id,"statement":query,"format":"JSON_ARRAY","disposition":"INLINE","wait_timeout":"0s","on_wait_timeout":"CONTINUE","byte_limit":25*1024*1024}),format!("{b}/api/2.0/sql/statements"))},
       Connection::Snowflake{host,warehouse,database,schema,role,token_env}=>{let b=host.trim_end_matches('/').to_string();(b.clone(),secret(token_env)?,json!({"warehouse":warehouse,"database":database,"schema":schema,"role":role,"statement":query,"timeout":remote.limits.query_timeout_secs,"parameters":{"MULTI_STATEMENT_COUNT":"1","TIMEZONE":"UTC","DATE_OUTPUT_FORMAT":"YYYY-MM-DD","TIMESTAMP_NTZ_OUTPUT_FORMAT":"YYYY-MM-DD\"T\"HH24:MI:SS.FF6","TIMESTAMP_LTZ_OUTPUT_FORMAT":"YYYY-MM-DD\"T\"HH24:MI:SS.FF6TZH:TZM","TIMESTAMP_TZ_OUTPUT_FORMAT":"YYYY-MM-DD\"T\"HH24:MI:SS.FF6TZH:TZM"}}),format!("{b}/api/v2/statements?async=true"))},
       Connection::Bigquery{project,location,token_env,maximum_bytes_billed}=>{
        safe_id(project)?;let b=format!("https://bigquery.googleapis.com/bigquery/v2/projects/{project}");
        (b.clone(),secret(token_env)?,json!({"query":query,"useLegacySql":false,"location":location,"maxResults":remote.limits.batch_rows,"timeoutMs":0,"requestId":uuid::Uuid::new_v4().to_string(),"maximumBytesBilled":maximum_bytes_billed.to_string(),"useQueryCache":false}),format!("{b}/queries"))
       },_=>bail!("invalid warehouse connection")
      };
      let value=http.request(Method::POST,&url,&token,Some(body),metrics,name).await?;
      let (id,poll,cancel)=match &remote.source.connection {
       Connection::Databricks{..}=>{let id=string(&value,"statement_id")?;safe_id(&id)?;(id.clone(),format!("{base}/api/2.0/sql/statements/{id}"),format!("{base}/api/2.0/sql/statements/{id}/cancel"))},
       Connection::Snowflake{..}=>{let id=string(&value,"statementHandle")?;safe_id(&id)?;(id.clone(),format!("{base}/api/v2/statements/{id}"),format!("{base}/api/v2/statements/{id}/cancel"))},
       Connection::Bigquery{location,..}=>{let id=string(&value["jobReference"],"jobId")?;safe_id(&id)?;let loc=url::form_urlencoded::byte_serialize(location.as_bytes()).collect::<String>();(id.clone(),format!("{base}/queries/{id}?location={loc}&maxResults={}",remote.limits.batch_rows),format!("{base}/jobs/{id}/cancel?location={loc}"))},_=>unreachable!()
      };
      let job=RemoteJob{http:http.clone(),url:cancel,token:token.clone(),metrics:metrics.clone(),name:name.clone(),finished:false};
      Ok((base,token,value,poll,id,job))
     }.await;
     let (base,token,mut value,poll,id,mut job)=result.map_err(df_error)?;
     let name=&remote.source.name;let http=remote.http.as_ref().ok_or_else(|| df_error("warehouse client missing"))?;let metrics=&remote.metrics;
     loop {
      if remote.cancel.is_cancelled(){Err(df_error("query cancelled"))?;}
      let ready=match remote.source.connection {
       Connection::Databricks{..}=>match value["status"]["state"].as_str(){Some("SUCCEEDED")=>true,Some("PENDING"|"RUNNING")=>false,_=>{Err(df_error("Databricks statement failed or cancelled; inspect vendor query history"))?;false}},
       Connection::Snowflake{..}=>value.get("resultSetMetaData").is_some(),
       Connection::Bigquery{..}=>{if value.get("errors").is_some(){Err(df_error("BigQuery statement failed; inspect vendor query history"))?;}value["jobComplete"].as_bool().unwrap_or(false)},_=>false
      };
      if ready{break;}tokio::time::sleep(Duration::from_millis(250)).await;
      value=http.request(Method::GET,&poll,&token,None,metrics,name).await.map_err(df_error)?;
     }
     let bq=matches!(remote.source.connection,Connection::Bigquery{..});
     let db=matches!(remote.source.connection,Connection::Databricks{..});
     if db&&value["manifest"]["truncated"].as_bool().unwrap_or(false){Err(df_error("Databricks result truncated; query rejected, reduce result size"))?;}
     let partitions=value["resultSetMetaData"]["partitionInfo"].as_array().map(Vec::len).unwrap_or(1);let mut partition=1;
     if let Some(n)=value["totalBytesProcessed"].as_str().and_then(|s|s.parse().ok()){metrics.update(name,|m|m.remote_bytes_processed=Some(n));}
     if db {value=value["result"].take();}
     loop {
      let payload=if db{&value["data_array"]}else if bq{&value["rows"]}else{&value["data"]};
      let mut page=rows(payload,bq).map_err(df_error)?;
      if bq{normalize_bq(&mut page,&schema).map_err(df_error)?;}
      for chunk in page.chunks(remote.limits.batch_rows){
       let b=arrow_util::batch(schema.clone(),chunk).map_err(df_error)?;
       metrics.update(name,|m|{m.rows+=b.num_rows() as u64;m.arrow_bytes+=b.get_array_memory_size() as u64;});yield b;
      }
      let next=if db {
       value["next_chunk_index"].as_u64().map(|n|format!("{base}/api/2.0/sql/statements/{id}/result/chunks/{n}"))
      }else if bq {
       value["pageToken"].as_str().filter(|s|!s.is_empty()).map(|p|format!("{poll}&pageToken={}",url::form_urlencoded::byte_serialize(p.as_bytes()).collect::<String>()))
      }else if partition<partitions{let url=format!("{poll}?partition={partition}");partition+=1;Some(url)}else{None};
      if let Some(next)=next{value=http.request(Method::GET,&next,&token,None,metrics,name).await.map_err(df_error)?;}else{break;}
     }
     job.finished=true;
     drop(job);
    };
    Ok(Box::pin(RecordBatchStreamAdapter::new(out, stream)))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn null_and_string_null_are_distinct() {
        let r = rows(&json!([[null, "null", "12345678901234567890"]]), false).unwrap();
        assert_eq!(r[0][0], None);
        assert_eq!(r[0][1], Some("null".into()));
    }
    #[test]
    fn rejects_nested() {
        assert!(rows(&json!([[{"secret":1}]]), false).is_err());
    }
}
