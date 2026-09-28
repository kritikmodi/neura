//! Real ClickHouse HTTP/Arrow tests, never connector mocks.
use futures::StreamExt;
use neura::{
    config::{Config, Source},
    engine::Engine,
};
use std::time::Duration;

fn source() -> Source {
    toml::from_str(r#"
name='ch'
kind='clickhouse'
endpoint='http://127.0.0.1:58123'
user='neura_reader'
password_env='NEURA_TEST_CLICKHOUSE_PASSWORD'
allow_http=true
consistency='statement'
[[tables]]
name='customers'
remote='neura.customers'
columns=[{name='id',data_type='int64',nullable=false},{name='region',data_type='utf8',nullable=false},{name='tenant_id',data_type='int64',nullable=false}]
[[tables]]
name='forbidden'
remote='neura.forbidden'
columns=[{name='id',data_type='int64',nullable=false}]
[[tables]]
name='stream_rows'
remote='neura.stream_rows'
columns=[{name='id',data_type='int64',nullable=false},{name='payload',data_type='utf8',nullable=false}]
[[tables]]
name='slow_rows'
remote='neura.slow_rows'
columns=[{name='id',data_type='int64',nullable=false},{name='pause',data_type='int64',nullable=false}]
[[tables]]
name='oversized'
remote='neura.oversized'
columns=[{name='payload',data_type='utf8',nullable=false}]
[[tables]]
name='types'
remote='neura.types'
columns=[{name='id',data_type='int64',nullable=false},{name='amount',data_type='decimal(38,2)',nullable=false},{name='day',data_type='date',nullable=false},{name='at',data_type='timestamptz',nullable=false},{name='missing',data_type='int64'}]
"#).unwrap()
}
fn config() -> Config {
    let mut c: Config = toml::from_str("").unwrap();
    c.limits.memory_mb = 64;
    c.limits.max_remote_page_mb = 1;
    c.limits.batch_rows = 128;
    c.limits.partitions = 2;
    c.sources.push(source());
    c
}
fn rows(b: &[datafusion::arrow::record_batch::RecordBatch]) -> Vec<Vec<Option<String>>> {
    b.iter()
        .flat_map(|b| (0..b.num_rows()).map(|i| neura::api::row(b, i).unwrap()))
        .collect()
}
async fn admin(sql: &str) -> String {
    let response = reqwest::Client::new()
        .post("http://127.0.0.1:58123")
        .basic_auth("fixture_admin", Some("neura-test-clickhouse-admin"))
        .body(sql.to_owned())
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    assert!(status.is_success(), "{status}: {body}");
    body
}
#[tokio::test]
#[ignore = "requires real ClickHouse; scripts/clickhouse-integration.sh"]
async fn governed_projection_filter_aggregate_and_types() {
    let e = Engine::new(config()).unwrap();
    let (id,b)=e.collect("SELECT region, COUNT(*) AS n FROM ch.customers WHERE id<10 GROUP BY region ORDER BY region").await.unwrap();
    assert_eq!(
        rows(&b),
        vec![
            vec![Some("east".into()), Some("4".into())],
            vec![Some("west".into()), Some("5".into())]
        ]
    );
    let d = e.diagnostic(&id).unwrap();
    assert_eq!(d.sources["ch"].rows, 2, "aggregate must run remotely");
    assert_eq!(d.sources["ch"].requests, 1);
    assert!(d.sources["ch"].response_bytes.unwrap() > 0);
    let (id, b) = e
        .collect("SELECT region FROM ch.customers WHERE id<10")
        .await
        .unwrap();
    assert_eq!(rows(&b).len(), 9);
    assert!(
        !e.diagnostic(&id)
            .unwrap()
            .physical_plan
            .unwrap()
            .contains("tenant_id")
    );
    let (_, b) = e
        .collect("SELECT COUNT(*) AS n FROM ch.customers")
        .await
        .unwrap();
    assert_eq!(
        rows(&b),
        vec![vec![Some("100".into())]],
        "source row policy must apply"
    );
    assert!(e.collect("SELECT * FROM ch.forbidden").await.is_err());
    assert!(e.collect("DELETE FROM ch.customers").await.is_err());
    let (_, b) = e.collect("SELECT * FROM ch.types").await.unwrap();
    let r = rows(&b);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][1].as_deref(), Some("12345678901234567890.12"));
    assert_eq!(r[0][2].as_deref(), Some("2026-01-02"));
    assert_eq!(r[0][3].as_deref(), Some("2026-01-02T03:04:05.123456Z"));
    assert_eq!(r[0][4], None);
    let (_, b) = e
        .collect("SELECT id FROM ch.customers WHERE id<0")
        .await
        .unwrap();
    assert!(rows(&b).is_empty());
    let (_,b)=e.collect("SELECT c.id, d.region FROM ch.customers c LEFT JOIN ch.customers d ON c.id=d.id+100 WHERE c.id=1").await.unwrap();
    assert_eq!(
        rows(&b),
        vec![vec![Some("1".into()), None]],
        "outer joins must produce NULL, not ClickHouse defaults"
    );
}
#[tokio::test]
#[ignore = "requires real ClickHouse; scripts/clickhouse-integration.sh"]
async fn streams_more_than_buffer_budget_and_cancels_source() {
    let mut c = config();
    // Default-size IPC batches share one allocation across columns. They must
    // fit a 1 MiB budget without double-counting each column's backing buffer.
    c.limits.batch_rows = 4096;
    let e = Engine::new(c).unwrap();
    let mut q = e
        .query("SELECT id, payload FROM ch.stream_rows")
        .await
        .unwrap();
    let mut count = 0usize;
    let mut sum = 0i64;
    while let Some(b) = q.stream.next().await {
        let b = b.unwrap();
        assert!(b.num_rows() <= 4096);
        let a = b
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap();
        count += b.num_rows();
        sum += a.values().iter().sum::<i64>();
    }
    assert_eq!(count, 200000);
    assert_eq!(sum, 199999 * 200000 / 2);
    assert!(
        e.diagnostic(&q.id).unwrap().sources["ch"]
            .response_bytes
            .unwrap()
            > 20 * 1024 * 1024
    );
    let e = Engine::new(config()).unwrap();
    let mut q = e.query("SELECT id, pause FROM ch.slow_rows").await.unwrap();
    let id = q.id.clone();
    let pending = tokio::spawn(async move {
        while let Some(b) = q.stream.next().await {
            b?;
        }
        Ok::<_, datafusion::error::DataFusionError>(())
    });
    let activity = "SELECT count() FROM system.processes WHERE user='neura_reader' AND query LIKE '%slow_rows%' AND query NOT LIKE 'KILL%'";
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if admin(activity).await.trim() != "0" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("ClickHouse query did not start");
    assert!(e.cancel(&id));
    assert!(
        tokio::time::timeout(Duration::from_secs(3), pending)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if admin(activity).await.trim() == "0" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("ClickHouse continued after cancellation");
}
#[tokio::test]
#[ignore = "requires real ClickHouse, PostgreSQL and S3 fixtures; scripts/integration.sh then scripts/clickhouse-integration.sh"]
async fn clickhouse_postgres_s3_join() {
    let mut c = Config::load("data/test.toml").unwrap();
    c.sources.push(source());
    let e = Engine::new(c).unwrap();
    let (_,b)=e.collect("SELECT c.region, SUM(o.amount) AS total FROM ch.customers c JOIN pg.customers p ON c.id=p.id JOIN lake.orders o ON p.id=o.customer_id GROUP BY c.region ORDER BY c.region").await.unwrap();
    let expected: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string("data/expected.json").unwrap()).unwrap();
    assert_eq!(serde_json::to_value(rows(&b)).unwrap(), expected["grouped"]);
    let result=e.collect("SELECT c.id FROM ch.customers c JOIN pg.customers p ON c.id=p.id UNION ALL SELECT d.id FROM ch.customers d JOIN pg.customers p ON d.id=p.id").await;
    let error = result.expect_err("two ClickHouse statements must not pretend to share a snapshot");
    assert!(
        error.to_string().contains("multiple remote statements"),
        "{error}"
    );
}
#[test]
fn clickhouse_requires_explicit_consistency_and_secure_endpoint() {
    let mut c = config();
    c.validate().unwrap();
    if let neura::config::Connection::Clickhouse { allow_http, .. } = &mut c.sources[0].connection {
        *allow_http = false;
    }
    assert!(c.validate().is_err());
    let mut c = config();
    c.sources[0].tables[0].snapshot = Some("2026-01-01T00:00:00Z".into());
    assert!(c.validate().is_err());
    let mut c = config();
    if let neura::config::Connection::Clickhouse { consistency, .. } = &mut c.sources[0].connection
    {
        *consistency = "repeatable_read".into();
    }
    assert!(c.validate().is_err());
}
#[test]
fn wire_is_explicitly_optional() {
    let c: Config = toml::from_str("").unwrap();
    assert!(c.server.pg_addr.is_none());
    let c: Config = toml::from_str("[server]\npg_addr='127.0.0.1:5433'").unwrap();
    assert_eq!(c.validate().is_ok(), cfg!(feature = "postgres-wire"));
}

#[tokio::test]
#[ignore = "requires real ClickHouse; scripts/clickhouse-integration.sh"]
async fn source_limits_and_oversized_frames_fail_closed() {
    let e = Engine::new(config()).unwrap();
    let error = e
        .collect("SELECT payload FROM ch.oversized")
        .await
        .expect_err("2 MiB value must not fit 1 MiB frame cap");
    assert!(error.to_string().contains("buffer limit"), "{error}");
    let mut c = config();
    c.limits.memory_mb = 1024;
    let e = Engine::new(c).unwrap();
    assert!(
        e.collect("SELECT id FROM ch.customers").await.is_err(),
        "source settings profile must remain authoritative"
    );
    let response = reqwest::Client::new()
        .post("http://127.0.0.1:58123")
        .basic_auth(
            "neura_reader",
            Some(std::env::var("NEURA_TEST_CLICKHOUSE_PASSWORD").unwrap()),
        )
        .body("INSERT INTO neura.customers VALUES (999,'forbidden',1)")
        .send()
        .await
        .unwrap();
    assert!(
        !response.status().is_success(),
        "source identity must reject writes independently of Neura validation"
    );
}
