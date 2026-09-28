use crate::{
    config::{Connection, Source, Table},
    connectors::cache::{CachedStore, ReadCache},
    metrics::Metrics,
};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use datafusion::{
    catalog::TableProvider,
    datasource::{
        file_format::parquet::ParquetFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    },
    prelude::SessionContext,
};
use futures::{
    StreamExt,
    stream::{self, BoxStream},
};
use iceberg::{NamespaceIdent, TableIdent, io::FileIO, table::StaticTable};
use iceberg_datafusion::table::IcebergStaticTableProvider;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    path::Path,
};
use std::{collections::HashMap, fmt, sync::Arc};

mod read_ahead;
pub use read_ahead::ReadAheadBudget;

#[derive(Clone)]
pub struct PinnedStore {
    inner: Arc<dyn ObjectStore>,
    pins: Arc<HashMap<Path, String>>,
    metrics: Metrics,
    name: String,
    read_ahead: Option<Arc<read_ahead::ReadAhead>>,
    measure_origin: bool,
}
impl fmt::Debug for PinnedStore {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("PinnedReadOnlyStore")
            .field("source", &self.name)
            .finish()
    }
}
impl fmt::Display for PinnedStore {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PinnedReadOnlyStore({})", self.name)
    }
}
fn denied() -> object_store::Error {
    object_store::Error::NotSupported {
        source: Box::new(std::io::Error::other(
            "Neura storage is read-only and manifest-scoped",
        )),
    }
}
impl PinnedStore {
    pub fn new(
        inner: Arc<dyn ObjectStore>,
        pins: HashMap<Path, String>,
        metrics: Metrics,
        name: String,
    ) -> Self {
        Self {
            inner,
            pins: Arc::new(pins),
            metrics,
            name,
            read_ahead: None,
            measure_origin: true,
        }
    }

    fn with_read_ahead(mut self, budget: Arc<ReadAheadBudget>, window: usize) -> Self {
        if window > 0 {
            self.read_ahead = Some(Arc::new(read_ahead::ReadAhead::new(
                self.pins.keys().cloned(),
                budget,
                window,
                self.metrics.clone(),
                self.name.clone(),
            )));
        }
        self
    }

    async fn fetch_opts(&self, path: &Path, opts: GetOptions) -> object_store::Result<GetResult> {
        if !self.measure_origin {
            return self.inner.get_opts(path, opts).await;
        }
        let timer = self.metrics.timer(&self.name);
        let result = self.inner.get_opts(path, opts).await?;
        let meta = result.meta.clone();
        let range = result.range.clone();
        let attributes = result.attributes.clone();
        let metrics = self.metrics.clone();
        let name = self.name.clone();
        let payload = async_stream::try_stream! {
            let _timer = timer;
            let mut input = result.into_stream();
            while let Some(bytes) = input.next().await {
                let bytes = bytes?;
                metrics.update(&name, |m| {
                    m.response_bytes = Some(m.response_bytes.unwrap_or(0) + bytes.len() as u64)
                });
                yield bytes;
            }
        }
        .boxed();
        Ok(GetResult {
            payload: GetResultPayload::Stream(payload),
            meta,
            range,
            attributes,
        })
    }
}
#[async_trait]
impl ObjectStore for PinnedStore {
    async fn put_opts(
        &self,
        _: &Path,
        _: PutPayload,
        _: PutOptions,
    ) -> object_store::Result<PutResult> {
        Err(denied())
    }
    async fn put_multipart_opts(
        &self,
        _: &Path,
        _: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(denied())
    }
    async fn copy_opts(&self, _: &Path, _: &Path, _: CopyOptions) -> object_store::Result<()> {
        Err(denied())
    }
    fn delete_stream(
        &self,
        _: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        stream::once(async { Err(denied()) }).boxed()
    }
    async fn get_opts(&self, path: &Path, mut opts: GetOptions) -> object_store::Result<GetResult> {
        let etag = self.pins.get(path).ok_or_else(denied)?;
        // A wildcard or alternative ETag list would turn a manifest pin into a
        // moving target and cannot be used as a read-ahead/cache identity.
        if etag.is_empty() || etag == "*" || etag.contains(',') {
            return Err(denied());
        }
        opts.if_match = Some(etag.clone());
        if let Some(read_ahead) = &self.read_ahead {
            read_ahead
                .get(path, opts, |opts| self.fetch_opts(path, opts))
                .await
        } else {
            self.fetch_opts(path, opts).await
        }
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let paths: Vec<_> = self
            .pins
            .keys()
            .filter(|p| prefix.is_none_or(|prefix| p.prefix_matches(prefix)))
            .cloned()
            .collect();
        let this = self.clone();
        stream::iter(paths)
            .then(move |p| {
                let this = this.clone();
                async move {
                    Ok(this
                        .get_opts(
                            &p,
                            GetOptions {
                                head: true,
                                ..Default::default()
                            },
                        )
                        .await?
                        .meta)
                }
            })
            .boxed()
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        use futures::TryStreamExt;
        Ok(ListResult {
            common_prefixes: vec![],
            objects: self.list(prefix).try_collect().await?,
        })
    }
}
pub async fn parquet(
    ctx: &SessionContext,
    source: &Source,
    metrics: Metrics,
    read_ahead: Option<(Arc<ReadAheadBudget>, usize)>,
    cache: Option<Arc<ReadCache>>,
) -> Result<Vec<(String, Arc<dyn TableProvider>)>> {
    let cached_s3 = cache.is_some() && matches!(&source.connection, Connection::S3 { .. });
    let (store, root): (Arc<dyn ObjectStore>, String) = match &source.connection {
        Connection::S3 {
            bucket,
            region,
            endpoint,
            allow_http,
            ..
        } => {
            let mut b = object_store::aws::AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .with_region(region)
                .with_allow_http(*allow_http);
            if let Some(endpoint) = endpoint {
                b = b
                    .with_endpoint(endpoint)
                    .with_virtual_hosted_style_request(false);
            }
            let s3 = Arc::new(b.build()?);
            let store: Arc<dyn ObjectStore> = if let Some(cache) = cache {
                Arc::new(CachedStore::s3(
                    s3,
                    cache,
                    source.name.clone(),
                    endpoint.as_deref(),
                    region,
                    bucket,
                    metrics.clone(),
                ))
            } else {
                s3
            };
            (store, format!("s3://{bucket}"))
        }
        Connection::Parquet {} => (
            Arc::new(object_store::local::LocalFileSystem::new()),
            "file://".into(),
        ),
        _ => unreachable!(),
    };
    let mut pins = HashMap::new();
    let mut paths = HashMap::new();
    for table in &source.tables {
        let mut urls = vec![];
        for f in &table.files {
            let (path, url) = if root == "file://" {
                let p = std::fs::canonicalize(&f.path)?;
                let url =
                    url::Url::from_file_path(&p).map_err(|_| anyhow::anyhow!("invalid path"))?;
                (Path::from_absolute_path(p)?, url.to_string())
            } else {
                let path = Path::parse(&f.path)?;
                (path, format!("{root}/{}", f.path))
            };
            let etag = if let Some(etag) = &f.etag {
                etag.clone()
            } else {
                let _timer = (!cached_s3).then(|| metrics.timer(&source.name));
                store
                    .head(&path)
                    .await?
                    .e_tag
                    .context("file store has no ETag; cannot pin file")?
            };
            ensure!(
                !etag.is_empty() && etag != "*" && !etag.contains(','),
                "manifest ETags must identify one concrete object version"
            );
            if let Some(old) = pins.insert(path, etag.clone()) {
                ensure!(old == etag, "conflicting ETags in manifest");
            }
            urls.push(ListingTableUrl::parse(url)?);
        }
        paths.insert(table.name.clone(), urls);
    }
    let mut wrapper = PinnedStore::new(store, pins, metrics, source.name.clone());
    wrapper.measure_origin = !cached_s3;
    if matches!(&source.connection, Connection::S3 { .. })
        && let Some((budget, window)) = read_ahead
    {
        wrapper = wrapper.with_read_ahead(budget, window);
    }
    let wrapper = Arc::new(wrapper);
    ctx.runtime_env()
        .register_object_store(&url::Url::parse(&root)?, wrapper);
    let mut result = vec![];
    for table in &source.tables {
        let opts =
            ListingOptions::new(Arc::new(ParquetFormat::default().with_enable_pruning(true)))
                .with_file_extension(".parquet")
                .with_collect_stat(true)
                .with_target_partitions(ctx.state().config().target_partitions());
        let config = ListingTableConfig::new_with_multi_paths(paths.remove(&table.name).unwrap())
            .with_listing_options(opts)
            .infer_schema(&ctx.state())
            .await?;
        result.push((
            table.name.clone(),
            Arc::new(ListingTable::try_new(config)?) as Arc<dyn TableProvider>,
        ));
    }
    Ok(result)
}
pub async fn iceberg(source: &Source, table: &Table) -> Result<Arc<dyn TableProvider>> {
    let loaded = match &source.connection {
        Connection::Iceberg {} => {
            let location = table
                .metadata_location
                .as_ref()
                .context("metadata file required")?;
            // Local static metadata is explicit; S3 Iceberg uses Glue so credentials and catalog authorization are checked.
            ensure!(
                !location.contains("://") || location.starts_with("file://"),
                "static Iceberg supports local metadata; use Glue for S3 Iceberg"
            );
            StaticTable::from_metadata_file(
                location,
                TableIdent::new(NamespaceIdent::new(source.name.clone()), table.name.clone()),
                FileIO::new_with_fs(),
            )
            .await?
            .into_table()
        }
        Connection::Glue {
            warehouse, region, ..
        } => {
            let parts = table
                .remote
                .as_ref()
                .unwrap()
                .split('.')
                .collect::<Vec<_>>();
            let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(aws_sdk_glue::config::Region::new(region.clone()))
                .load()
                .await;
            use aws_sdk_glue::config::ProvideCredentials;
            let credentials = config
                .credentials_provider()
                .context("AWS credential provider missing")?
                .provide_credentials()
                .await
                .map_err(|_| anyhow::anyhow!("AWS credentials unavailable"))?;
            // Freeze the same resolved identity for catalog and storage access.
            // Do not let a second provider chain silently select a different role.
            let glue_config = aws_sdk_glue::config::Builder::from(&config)
                .credentials_provider(credentials.clone())
                .build();
            let response = aws_sdk_glue::Client::from_conf(glue_config)
                .get_table()
                .database_name(parts[0])
                .name(parts[1])
                .send()
                .await
                .map_err(|_| {
                    anyhow::anyhow!(
                        "Glue GetTable failed; check catalog authorization and connectivity"
                    )
                })?;
            let t = response.table().context("Glue table missing")?;
            ensure!(
                !t.is_registered_with_lake_formation(),
                "Lake Formation registered tables are refused: credential vending is not implemented"
            );
            let parameters = t.parameters().context("Glue Iceberg parameters missing")?;
            ensure!(
                parameters
                    .get("table_type")
                    .is_some_and(|v| v.eq_ignore_ascii_case("ICEBERG")),
                "only Iceberg Glue tables are supported"
            );
            let location = parameters
                .get("metadata_location")
                .context("Glue metadata_location missing")?;
            ensure!(
                location.starts_with(&format!("{}/", warehouse.trim_end_matches('/'))),
                "Glue metadata is outside the configured warehouse scope"
            );
            let mut io = iceberg::io::FileIOBuilder::new(Arc::new(
                iceberg_storage_opendal::OpenDalStorageFactory::S3 {
                    customized_credential_load: None,
                },
            ))
            .with_prop(iceberg::io::S3_REGION, region)
            .with_prop(iceberg::io::S3_ACCESS_KEY_ID, credentials.access_key_id())
            .with_prop(
                iceberg::io::S3_SECRET_ACCESS_KEY,
                credentials.secret_access_key(),
            )
            .with_prop(iceberg::io::S3_DISABLE_CONFIG_LOAD, "true")
            .with_prop(iceberg::io::S3_DISABLE_EC2_METADATA, "true");
            if let Some(token) = credentials.session_token() {
                io = io.with_prop(iceberg::io::S3_SESSION_TOKEN, token);
            }
            let io = io.build();
            StaticTable::from_metadata_file(
                location,
                TableIdent::new(NamespaceIdent::new(parts[0].into()), parts[1].into()),
                io,
            )
            .await?
            .into_table()
        }
        _ => unreachable!(),
    };
    let provider = if let Some(id) = table.snapshot_id {
        IcebergStaticTableProvider::try_new_from_table_snapshot(loaded, id).await?
    } else {
        IcebergStaticTableProvider::try_new_from_table(loaded).await?
    };
    Ok(Arc::new(provider))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn rejects_changed_object_and_writes() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let p = Path::from("x.parquet");
        store.put(&p, "first".into()).await.unwrap();
        let tag = store.head(&p).await.unwrap().e_tag.unwrap();
        let pinned = PinnedStore::new(
            store.clone(),
            HashMap::from([(p.clone(), tag)]),
            Metrics::default(),
            "test".into(),
        );
        assert!(pinned.get(&p).await.is_ok());
        store.put(&p, "second".into()).await.unwrap();
        assert!(pinned.get(&p).await.is_err());
        assert!(pinned.put(&p, "bad".into()).await.is_err());
        assert!(pinned.get(&Path::from("other")).await.is_err());
    }

    #[tokio::test]
    async fn read_ahead_is_manifest_scoped_and_never_shared_between_queries() {
        use datafusion::execution::memory_pool::GreedyMemoryPool;
        let store = Arc::new(object_store::memory::InMemory::new());
        let path = Path::from("orders.parquet");
        let secret = Path::from("not_in_manifest.parquet");
        store.put(&path, vec![1; 128].into()).await.unwrap();
        store.put(&secret, vec![9; 128].into()).await.unwrap();
        let tag = store.head(&path).await.unwrap().e_tag.unwrap();
        let metrics = Metrics::default();
        let budget = Arc::new(ReadAheadBudget::new(
            Arc::new(GreedyMemoryPool::new(128)),
            128,
        ));
        let first = PinnedStore::new(
            store.clone(),
            HashMap::from([(path.clone(), tag.clone())]),
            metrics.clone(),
            "s3".into(),
        )
        .with_read_ahead(budget.clone(), 32);
        for start in [0, 4, 8, 12] {
            assert_eq!(
                first
                    .get_range(&path, start..start + 4)
                    .await
                    .unwrap()
                    .as_ref(),
                &[1; 4]
            );
        }
        assert_eq!(metrics.snapshot()["s3"].requests, 3);
        assert_eq!(metrics.snapshot()["s3"].read_ahead_hits, 1);
        assert!(first.get_range(&secret, 12..16).await.is_err());
        assert_eq!(metrics.snapshot()["s3"].requests, 3);
        store.put(&path, vec![2; 128].into()).await.unwrap();
        let second = PinnedStore::new(
            store,
            HashMap::from([(path.clone(), tag)]),
            metrics,
            "s3".into(),
        )
        .with_read_ahead(budget, 32);
        assert!(matches!(
            second.get_range(&path, 12..16).await,
            Err(object_store::Error::Precondition { .. })
        ));
    }

    #[tokio::test]
    async fn rejects_wildcard_and_multi_version_manifest_pins() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let path = Path::from("orders.parquet");
        store.put(&path, "rows".into()).await.unwrap();
        for tag in ["*", "one,two", ""] {
            let pinned = PinnedStore::new(
                store.clone(),
                HashMap::from([(path.clone(), tag.into())]),
                Metrics::default(),
                "s3".into(),
            );
            assert!(pinned.get(&path).await.is_err());
        }
    }
}
