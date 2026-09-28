use neura::{config::Config, engine::Engine};
#[tokio::test]
#[ignore = "run scripts/iceberg_delete_fixture.py with pyiceberg and pyarrow"]
async fn position_and_equality_delete_files_preserve_old_snapshot() {
    let e = Engine::new(Config::load("data/iceberg-deletes.toml").unwrap()).unwrap();
    for (table, expected) in [
        ("old", vec!["1", "2", "3", "4"]),
        ("current", vec!["1", "4"]),
    ] {
        let (_, batches) = e
            .collect(&format!("SELECT id FROM ice.{table} ORDER BY id"))
            .await
            .unwrap();
        let actual: Vec<String> = batches
            .iter()
            .flat_map(|b| {
                (0..b.num_rows()).map(|i| neura::api::row(b, i).unwrap()[0].clone().unwrap())
            })
            .collect();
        assert_eq!(actual, expected);
    }
}
#[tokio::test]
#[ignore = "run scripts/iceberg_fixture.py with pyiceberg and pyarrow"]
async fn snapshot_schema_evolution_and_delete() {
    let e = Engine::new(Config::load("data/iceberg.toml").unwrap()).unwrap();
    let (_, b) = e
        .collect("SELECT SUM(amount) AS total FROM ice.old")
        .await
        .unwrap();
    assert_eq!(neura::api::row(&b[0], 0).unwrap(), vec![Some("600".into())]);
    let (_, b) = e
        .collect("SELECT SUM(total) AS total, COUNT(*) AS n FROM ice.current")
        .await
        .unwrap();
    assert_eq!(
        neura::api::row(&b[0], 0).unwrap(),
        vec![Some("1300".into()), Some("4".into())]
    );
    let (_, b) = e
        .collect(
            "SELECT o.id FROM ice.old o LEFT JOIN ice.current c ON o.id=c.id WHERE c.id IS NULL",
        )
        .await
        .unwrap();
    assert_eq!(neura::api::row(&b[0], 0).unwrap(), vec![Some("2".into())]);
}

#[tokio::test]
async fn memory_limited_sort_spills_without_changing_results() {
    let mut c: Config = toml::from_str("").unwrap();
    c.limits.memory_mb = 64;
    c.limits.sort_merge_reservation_mb = 32;
    c.limits.partitions = 1;
    c.limits.max_output_mb = 128;
    let e = Engine::new(c).unwrap();
    let mut q = e
        .query("SELECT value FROM generate_series(1, 4000000) ORDER BY value DESC")
        .await
        .unwrap();
    use futures::StreamExt;
    let mut rows = 0;
    let mut previous: Option<i64> = None;
    let mut sum = 0i64;
    while let Some(b) = q.stream.next().await {
        let b = b.unwrap();
        for i in 0..b.num_rows() {
            let value: i64 = neura::api::row(&b, i).unwrap()[0]
                .as_ref()
                .unwrap()
                .parse()
                .unwrap();
            let key = value;
            if let Some(p) = &previous {
                assert!(p >= &key);
            }
            previous = Some(key);
            sum += value;
        }
        rows += b.num_rows();
    }
    assert_eq!(rows, 4000000);
    assert_eq!(sum, 4000000i64 * 4000001 / 2);
    let p = e.diagnostic(&q.id).unwrap().physical_plan.unwrap();
    assert!(
        p.contains("spill_count=") && !p.contains("spill_count=0,"),
        "expected actual sort spilling: {p}"
    );
}
