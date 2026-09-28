use futures::StreamExt;
use neura::{config::Config, engine::Engine};
fn fixture() -> std::sync::Arc<Engine> {
    Engine::new(Config::load("data/test.toml").expect("run scripts/integration.sh first")).unwrap()
}
#[tokio::test]
#[ignore = "requires real PostgreSQL and S3 fixture; scripts/integration.sh"]
async fn postgres_s3_join_and_pushdown() {
    let e = fixture();
    let(id,b)=e.collect("SELECT c.region, SUM(o.amount) AS total FROM pg.customers c JOIN lake.orders o ON c.id=o.customer_id GROUP BY c.region ORDER BY c.region").await.unwrap();
    let rows = b
        .iter()
        .flat_map(|b| (0..b.num_rows()).map(|i| neura::api::row(b, i).unwrap()))
        .collect::<Vec<_>>();
    let expected: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string("data/expected.json").unwrap()).unwrap();
    assert_eq!(serde_json::to_value(rows).unwrap(), expected["grouped"]);
    let d = e.diagnostic(&id).unwrap();
    assert_eq!(d.status, "succeeded");
    assert!(d.sources["lake"].response_bytes.unwrap() > 0);
    assert!(d.sources["pg"].rows <= 100, "RLS must remove tenant 2");
    let (id, _) = e
        .collect("SELECT region, COUNT(*) AS n FROM pg.customers WHERE id<10 GROUP BY region")
        .await
        .unwrap();
    let d = e.diagnostic(&id).unwrap();
    assert!(d.physical_plan.unwrap().contains("GROUP BY"));
    assert!(
        d.sources["pg"].rows <= 2,
        "aggregation should be executed in PostgreSQL"
    );
    let (id, _) = e
        .collect("SELECT region FROM pg.customers WHERE id<10")
        .await
        .unwrap();
    let d = e.diagnostic(&id).unwrap();
    assert!(!d.physical_plan.unwrap().contains("tenant_id"));
    assert_eq!(
        d.sources["pg"].rows, 9,
        "remote filter should remove rows before transfer"
    );
    let (id, _) = e
        .collect("SELECT COUNT(*) AS n, SUM(amount) AS total FROM lake.orders WHERE order_id<1000")
        .await
        .unwrap();
    let d = e.diagnostic(&id).unwrap();
    assert!(
        d.sources["lake"].rows <= 1024,
        "filter must reach the Parquet reader; scanned {} rows",
        d.sources["lake"].rows
    );
    assert!(
        d.sources["lake"].requests < 30,
        "pruning must avoid fetching all row groups"
    );
}
#[tokio::test]
#[ignore = "requires real fixture"]
async fn source_permission_is_preserved() {
    let e = fixture();
    assert!(e.collect("SELECT * FROM pg.forbidden").await.is_err());
    let (_, b) = e
        .collect("SELECT COUNT(*) AS n FROM pg.customers")
        .await
        .unwrap();
    assert_eq!(neura::api::row(&b[0], 0).unwrap(), vec![Some("100".into())]);
    let mut c = Config::load("data/test.toml").unwrap();
    let lake = c.sources.iter_mut().find(|s| s.name == "lake").unwrap();
    if let neura::config::Connection::S3 { bucket, .. } = &mut lake.connection {
        *bucket = "neura-private".into();
    }
    lake.tables[0].files = vec![neura::config::FilePin {
        path: "private.parquet".into(),
        etag: Some(
            serde_json::from_str(&std::fs::read_to_string("data/private-etag.json").unwrap())
                .unwrap(),
        ),
    }];
    let e = Engine::new(c).unwrap();
    let error = e
        .collect("SELECT * FROM lake.orders")
        .await
        .expect_err("S3 must deny this bucket");
    assert!(
        error.to_string().contains("403")
            || error.to_string().contains("AccessDenied")
            || error.to_string().contains("Forbidden"),
        "unexpected error: {error}"
    );
}
#[tokio::test]
#[ignore = "requires real fixture"]
async fn concurrency_and_cancel() {
    let e = fixture();
    let q = e.query("SELECT * FROM lake.orders").await.unwrap();
    let id = q.id.clone();
    let q2 = e.query("SELECT 1").await.unwrap();
    assert!(e.query("SELECT 2").await.is_err());
    assert!(e.cancel(&id));
    let mut s = q.stream;
    assert!(s.next().await.unwrap().is_err());
    drop(q2);
    assert!(e.collect("SELECT 3").await.is_ok());
}

#[tokio::test]
#[ignore = "requires real PostgreSQL and S3 fixtures"]
async fn early_join_completion_returns_postgres_connection_cleanly() {
    let e = fixture();
    for _ in 0..3 {
        let (_, rows) = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            e.collect("SELECT o.order_id, c.region FROM pg.customers c JOIN lake.orders o ON c.id=o.customer_id LIMIT 1"),
        ).await.expect("LIMIT query left source cleanup waiting").unwrap();
        assert_eq!(rows.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        let (_, count) = e
            .collect("SELECT COUNT(*) FROM pg.customers")
            .await
            .unwrap();
        assert_eq!(
            neura::api::row(&count[0], 0).unwrap(),
            vec![Some("100".into())]
        );
    }
}
#[tokio::test]
#[ignore = "requires NEURA_CLOUD_CONFIG and actual cloud credentials; never mocked"]
async fn cloud_connectors() {
    let path = std::env::var("NEURA_CLOUD_CONFIG").expect("NEURA_CLOUD_CONFIG missing");
    let c = Config::load(path).unwrap();
    for required in ["databricks", "snowflake", "bigquery", "glue"] {
        assert!(
            c.sources.iter().any(|s| s.connection.kind() == required),
            "missing required live source {required}"
        );
    }
    let tables = c
        .sources
        .iter()
        .flat_map(|s| {
            s.tables
                .iter()
                .map(move |t| format!("{}.{}", s.name, t.name))
        })
        .collect::<Vec<_>>();
    let e = Engine::new(c).unwrap();
    for t in tables {
        let (_, b) = e
            .collect(&format!("SELECT COUNT(*) AS n FROM {t}"))
            .await
            .unwrap();
        assert_eq!(b.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    }
}

#[tokio::test]
#[ignore = "requires real PostgreSQL fixture"]
async fn repeatable_read_pins_the_query_snapshot() {
    let (admin, connection) = tokio_postgres::connect(
        "host=127.0.0.1 port=55432 user=postgres password=neura-test-admin-only dbname=neura",
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    admin.batch_execute("CREATE TABLE IF NOT EXISTS snapshot_probe (id bigint PRIMARY KEY, value bigint); INSERT INTO snapshot_probe VALUES (1, 10) ON CONFLICT (id) DO UPDATE SET value=10; GRANT SELECT ON snapshot_probe TO neura_reader").await.unwrap();
    let mut c = Config::load("data/test.toml").unwrap();
    let table: neura::config::Table=toml::from_str("name='snapshot_probe'\nremote='public.snapshot_probe'\ncolumns=[{name='id',data_type='int64'},{name='value',data_type='int64'}]").unwrap();
    c.sources
        .iter_mut()
        .find(|s| s.name == "pg")
        .unwrap()
        .tables
        .push(table);
    let e = Engine::new(c).unwrap();
    let mut q = e
        .query("SELECT value FROM pg.snapshot_probe WHERE id=1")
        .await
        .unwrap();
    admin
        .execute("UPDATE snapshot_probe SET value=20 WHERE id=1", &[])
        .await
        .unwrap();
    let batch = q.stream.next().await.unwrap().unwrap();
    assert_eq!(neura::api::row(&batch, 0).unwrap(), vec![Some("10".into())]);
    while q.stream.next().await.is_some() {}
    let (_, b) = e
        .collect("SELECT value FROM pg.snapshot_probe WHERE id=1")
        .await
        .unwrap();
    assert_eq!(neura::api::row(&b[0], 0).unwrap(), vec![Some("20".into())]);
}

#[tokio::test]
#[ignore = "requires real PostgreSQL fixture"]
async fn cancellation_stops_an_active_postgres_statement() {
    let (admin, connection) = tokio_postgres::connect(
        "host=127.0.0.1 port=55432 user=postgres password=neura-test-admin-only dbname=neura",
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    admin.batch_execute("CREATE OR REPLACE VIEW slow_probe AS SELECT 1::bigint AS id FROM pg_sleep(30); GRANT SELECT ON slow_probe TO neura_reader").await.unwrap();
    let mut c = Config::load("data/test.toml").unwrap();
    c.sources.iter_mut().find(|s| s.name == "pg").unwrap().tables.push(
        toml::from_str("name='slow_probe'\nremote='public.slow_probe'\ncolumns=[{name='id',data_type='int64'}]").unwrap()
    );
    let e = Engine::new(c).unwrap();
    let mut q = e.query("SELECT id FROM pg.slow_probe").await.unwrap();
    let id = q.id.clone();
    let pending = tokio::spawn(async move { q.stream.next().await.unwrap() });
    let activity = "SELECT COUNT(*) FROM pg_stat_activity WHERE usename='neura_reader' AND state='active' AND query LIKE '%slow_probe%' AND query NOT LIKE 'EXPLAIN%'";
    let active = async {
        loop {
            if admin
                .query_one(activity, &[])
                .await
                .unwrap()
                .get::<_, i64>(0)
                > 0
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), active)
        .await
        .expect("remote query never started");
    assert!(e.cancel(&id));
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(3), pending)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    let stopped = async {
        loop {
            if admin
                .query_one(activity, &[])
                .await
                .unwrap()
                .get::<_, i64>(0)
                == 0
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    };
    tokio::time::timeout(std::time::Duration::from_secs(3), stopped)
        .await
        .expect("remote statement continued after cancellation");
}
