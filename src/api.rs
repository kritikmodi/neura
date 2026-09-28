use crate::{config::secret, engine::Engine};
use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Path, Request, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use futures::StreamExt;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use subtle::ConstantTimeEq;
#[derive(Clone)]
struct App {
    engine: Arc<Engine>,
    token_hash: [u8; 32],
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryRequest {
    sql: String,
    id: Option<String>,
}
pub fn row(
    batch: &datafusion::arrow::record_batch::RecordBatch,
    index: usize,
) -> anyhow::Result<Vec<Option<String>>> {
    batch
        .columns()
        .iter()
        .map(|a| {
            if a.is_null(index) {
                Ok(None)
            } else {
                Ok(Some(
                    datafusion::arrow::util::display::array_value_to_string(a, index)?,
                ))
            }
        })
        .collect()
}
async fn auth(State(app): State<App>, req: Request, next: Next) -> Response {
    let token = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");
    let got: [u8; 32] = Sha256::digest(token.as_bytes()).into();
    if bool::from(got.ct_eq(&app.token_hash)) {
        next.run(req).await
    } else {
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error":"unauthorized"})),
        )
            .into_response()
    }
}
async fn query(State(app): State<App>, Json(req): Json<QueryRequest>) -> Response {
    let q = match app
        .engine
        .query_with_id(
            &req.sql,
            req.id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
        )
        .await
    {
        Ok(q) => q,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error":e.to_string()})),
            )
                .into_response();
        }
    };
    let id = q.id.clone();
    let header_id = id.clone();
    let mut batches = q.stream;
    let schema=q.schema.fields().iter().map(|f|json!({"name":f.name(),"type":f.data_type().to_string(),"nullable":f.is_nullable()})).collect::<Vec<_>>();
    let stream = async_stream::stream! {
     yield Ok::<_,std::io::Error>(format!("{}\n",json!({"type":"schema","id":id,"columns":schema})));
     while let Some(batch)=batches.next().await{
      let batch=match batch{Ok(b)=>b,Err(e)=>{yield Ok(format!("{}\n",json!({"type":"error","id":id,"error":e.to_string()})));return;}};
      for i in 0..batch.num_rows(){match row(&batch,i){Ok(r)=>yield Ok(format!("{}\n",json!({"type":"row","values":r}))),Err(_)=>{yield Ok(format!("{}\n",json!({"type":"error","id":id,"error":"result encoding failed"})));return;}}}
     }
     yield Ok(format!("{}\n",json!({"type":"complete","diagnostic":app.engine.diagnostic(&id)})));
    };
    (
        [
            (header::CONTENT_TYPE, "application/x-ndjson"),
            (header::CACHE_CONTROL, "no-store"),
            (
                header::HeaderName::from_static("x-neura-query-id"),
                header_id.as_str(),
            ),
        ],
        Body::from_stream(stream),
    )
        .into_response()
}
async fn sources(State(app): State<App>) -> Json<Value> {
    Json(
        json!({"sources":app.engine.config.sources.iter().map(|s|json!({"name":s.name,"kind":s.connection.kind(),"tables":s.tables.iter().map(|t|&t.name).collect::<Vec<_>>()})).collect::<Vec<_>>() }),
    )
}
async fn queries(State(app): State<App>) -> Json<Value> {
    Json(json!(app.engine.diagnostics()))
}
async fn diagnostic(State(app): State<App>, Path(id): Path<String>) -> Response {
    match app.engine.diagnostic(&id) {
        Some(d) => Json(json!(d)).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}
async fn cancel(State(app): State<App>, Path(id): Path<String>) -> Response {
    if app.engine.cancel(&id) {
        StatusCode::ACCEPTED.into_response()
    } else {
        StatusCode::NOT_FOUND.into_response()
    }
}
pub fn router(engine: Arc<Engine>) -> anyhow::Result<Router> {
    let token = secret(&engine.config.server.token_env)?;
    anyhow::ensure!(token.len() >= 24, "API token must be at least 24 bytes");
    let app = App {
        engine,
        token_hash: Sha256::digest(token.as_bytes()).into(),
    };
    Ok(Router::new()
        .route("/v1/query", post(query))
        .route("/v1/sources", get(sources))
        .route("/v1/queries", get(queries))
        .route("/v1/queries/{id}", get(diagnostic).merge(delete(cancel)))
        .layer(middleware::from_fn_with_state(app.clone(), auth))
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .with_state(app)
        .route(
            "/healthz",
            get(|| async { Json(json!({"status":"ok","version":env!("CARGO_PKG_VERSION")})) }),
        ))
}
pub async fn serve(engine: Arc<Engine>) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(&engine.config.server.http_addr).await?;
    axum::serve(listener, router(engine)?).await?;
    Ok(())
}
