use anyhow::{Result, ensure};
use neura::{config::Config, engine::Engine};
use object_store::{ObjectStoreExt, aws::AmazonS3Builder, path::Path};

/// Uses its own object key; never rewrites the benchmark or integration fixtures.
#[tokio::test]
#[ignore = "requires the real PostgreSQL/S3 fixture and scripts/test-env.sh"]
async fn cached_s3_reads_reject_replaced_and_deleted_objects() -> Result<()> {
    let admin = AmazonS3Builder::new()
        .with_bucket_name("neura-fixture")
        .with_region("us-east-1")
        .with_endpoint("http://127.0.0.1:59000")
        .with_allow_http(true)
        .with_access_key_id("neura-test")
        .with_secret_access_key("neura-test-storage-only")
        .build()?;
    let path = Path::from(format!("cache-test/{}/data.parquet", uuid::Uuid::new_v4()));
    let original = std::fs::read("data/part-0.parquet")?;
    let replacement = std::fs::read("data/part-1.parquet")?;
    let etag = admin.put(&path, original.into()).await?.e_tag.unwrap();
    let result = async {
        let mut config = Config::load("data/test.toml")?;
        config.sources.retain(|source| source.name == "lake");
        config.sources[0].tables.truncate(1);
        config.sources[0].tables[0].files = vec![neura::config::FilePin {
            path: path.to_string(),
            etag: Some(etag),
        }];
        config.limits.file_cache_mb = 32;
        let engine = Engine::new(config.clone())?;
        let sql = "SELECT COUNT(*) AS n, SUM(order_id) AS total FROM lake.orders";
        let (_, first) = engine.collect(sql).await?;
        let (id, second) = engine.collect(sql).await?;
        ensure!(neura::api::row(&first[0], 0)? == neura::api::row(&second[0], 0)?);
        ensure!(
            engine.diagnostic(&id).unwrap().sources["lake"].cache_hits > 0,
            "the test must exercise a populated reusable cache"
        );

        let new_etag = admin.put(&path, replacement.into()).await?.e_tag.unwrap();
        let error = engine
            .collect(sql)
            .await
            .expect_err("old manifest must fail after replacement");
        ensure!(
            error.to_string().contains("412")
                || error.to_string().contains("Precondition")
                || error.to_string().contains("precondition"),
            "unexpected replacement error: {error}"
        );

        config.sources[0].tables[0].files[0].etag = Some(new_etag);
        let updated = Engine::new(config)?;
        let (_, current) = updated.collect(sql).await?;
        ensure!(
            neura::api::row(&first[0], 0)? != neura::api::row(&current[0], 0)?,
            "new manifest must see the replacement data"
        );
        updated.collect(sql).await?;
        admin.delete(&path).await?;
        let error = updated
            .collect(sql)
            .await
            .expect_err("cached bytes must not hide deletion");
        ensure!(
            error.to_string().contains("404")
                || error.to_string().contains("not found")
                || error.to_string().contains("NotFound"),
            "unexpected deletion error: {error}"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let _ = admin.delete(&path).await;
    result
}
