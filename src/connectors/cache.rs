//! Bounded, process-local cache of immutable, explicitly pinned S3 objects.
//!
//! A cache lookup is never an authorization decision: every use issues a live
//! conditional GetObject request, including its body, under the current source
//! credentials. Already delivered buffers have the same lifetime as ordinary
//! ranged reads; this does not promise revocation of bytes already delivered.
//! Glue/Iceberg and unpinned requests do not use this cache.
use crate::metrics::Metrics;
use async_trait::async_trait;
use bytes::Bytes;
use futures::{
    StreamExt, TryStreamExt,
    stream::{self, BoxStream},
};
use object_store::{
    Attributes, CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload,
    PutResult,
    aws::{AmazonS3, AwsCredentialProvider},
    path::Path,
};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

const MAX_ENTRIES: usize = 1024;

#[derive(Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    scope: [u8; 32],
    path: Path,
    etag: String,
    version: Option<String>,
}
struct Entry {
    data: Bytes,
    touched: u64,
}
#[derive(Default)]
struct State {
    entries: HashMap<CacheKey, Entry>,
    clock: u64,
}
struct Budget {
    capacity: usize,
    used: AtomicUsize,
}
struct Reservation {
    budget: Arc<Budget>,
    bytes: usize,
}
impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}
struct OwnedPayload {
    data: Box<[u8]>,
    _reservation: Reservation,
}
impl AsRef<[u8]> for OwnedPayload {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

/// The limit includes fills and backing allocations retained by returned slices,
/// even after their cache entry is evicted. Metadata has a separate 1024-entry
/// bound; transport buffers and downstream decoded Arrow arrays are not cache
/// allocations and belong to their respective runtime budgets.
pub struct ReadCache {
    budget: Arc<Budget>,
    max_object_bytes: usize,
    state: Mutex<State>,
    // Only one fill can run; other misses stream from origin without queuing.
    // Hits never wait behind a download. No per-key waiter map is retained.
    fill: tokio::sync::Mutex<()>,
}
impl fmt::Debug for ReadCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadCache")
            .field("capacity", &self.budget.capacity)
            .field("resident_bytes", &self.resident_bytes())
            .finish()
    }
}
impl ReadCache {
    pub fn new(capacity_bytes: usize, max_object_bytes: usize) -> Self {
        Self {
            budget: Arc::new(Budget {
                capacity: capacity_bytes,
                used: AtomicUsize::new(0),
            }),
            max_object_bytes: max_object_bytes.min(capacity_bytes),
            state: Mutex::new(State::default()),
            fill: tokio::sync::Mutex::new(()),
        }
    }
    pub fn resident_bytes(&self) -> usize {
        self.budget.used.load(Ordering::Acquire)
    }
    fn get(&self, key: &CacheKey) -> Option<Bytes> {
        let mut state = self.state.lock().unwrap();
        state.clock = state.clock.wrapping_add(1);
        let clock = state.clock;
        let entry = state.entries.get_mut(key)?;
        entry.touched = clock;
        Some(entry.data.clone())
    }
    fn reserve(&self, bytes: usize) -> (Option<Reservation>, u64) {
        if bytes == 0 || bytes > self.max_object_bytes {
            return (None, 0);
        }
        let mut state = self.state.lock().unwrap();
        let mut evictions = 0;
        loop {
            let used = self.resident_bytes();
            if used <= self.budget.capacity - bytes && state.entries.len() < MAX_ENTRIES {
                // All increases hold state; releases only decrease the atomic.
                self.budget.used.fetch_add(bytes, Ordering::AcqRel);
                return (
                    Some(Reservation {
                        budget: self.budget.clone(),
                        bytes,
                    }),
                    evictions,
                );
            }
            let oldest = state
                .entries
                .iter()
                .min_by_key(|(_, e)| e.touched)
                .map(|(key, _)| key.clone());
            let Some(oldest) = oldest else {
                return (None, evictions);
            };
            state.entries.remove(&oldest);
            evictions += 1;
        }
    }
    fn insert(&self, key: CacheKey, data: Bytes) {
        let mut state = self.state.lock().unwrap();
        state.clock = state.clock.wrapping_add(1);
        let touched = state.clock;
        state.entries.insert(key, Entry { data, touched });
    }
}

/// Read-only wrapper used only by the manifest-scoped S3 Parquet connector.
/// The AWS provider remains dynamic; its resolved credential identity (including
/// session token) participates in a one-way cache scope on every operation.
#[derive(Clone)]
pub struct CachedStore {
    inner: Arc<dyn ObjectStore>,
    cache: Arc<ReadCache>,
    base_scope: [u8; 32],
    credentials: Option<AwsCredentialProvider>,
    metrics: Metrics,
    name: String,
}
impl fmt::Debug for CachedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthorizedReadCache")
            .field("source", &self.name)
            .finish()
    }
}
impl fmt::Display for CachedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AuthorizedReadCache({})", self.name)
    }
}
fn digest_parts(parts: &[&[u8]]) -> [u8; 32] {
    let mut hash = Sha256::new();
    for part in parts {
        hash.update((part.len() as u64).to_le_bytes());
        hash.update(part);
    }
    hash.finalize().into()
}
fn invalid(message: &str) -> object_store::Error {
    object_store::Error::Generic {
        store: "NeuraReadCache",
        source: std::io::Error::other(message).into(),
    }
}
fn denied() -> object_store::Error {
    object_store::Error::NotSupported {
        source: std::io::Error::other("Neura cache is read-only").into(),
    }
}
impl CachedStore {
    pub fn s3(
        inner: Arc<AmazonS3>,
        cache: Arc<ReadCache>,
        source: String,
        endpoint: Option<&str>,
        region: &str,
        bucket: &str,
        metrics: Metrics,
    ) -> Self {
        let base_scope = digest_parts(&[
            source.as_bytes(),
            endpoint.unwrap_or("AWS_DEFAULT_ENDPOINT").as_bytes(),
            region.as_bytes(),
            bucket.as_bytes(),
        ]);
        let credentials = Some(inner.credentials().clone());
        Self {
            inner,
            cache,
            base_scope,
            credentials,
            metrics,
            name: source,
        }
    }
    async fn scope(&self) -> Option<[u8; 32]> {
        if let Some(provider) = &self.credentials {
            let credential = provider.get_credential().await.ok()?;
            Some(digest_parts(&[
                &self.base_scope,
                credential.key_id.as_bytes(),
                credential.secret_key.as_bytes(),
                credential.token.as_deref().unwrap_or("").as_bytes(),
            ]))
        } else {
            // Only unit tests construct a store without an AWS provider.
            Some(self.base_scope)
        }
    }
    async fn origin(&self, path: &Path, opts: GetOptions) -> object_store::Result<GetResult> {
        let timer = self.metrics.timer(&self.name);
        let result = self.inner.get_opts(path, opts).await?;
        let meta = result.meta.clone();
        let range = result.range.clone();
        let attributes = result.attributes.clone();
        let metrics = self.metrics.clone();
        let name = self.name.clone();
        // The timer lives until response consumption or cancellation, not just
        // until headers arrive. Response bytes count actual network bodies.
        let payload = async_stream::try_stream! {
            let _timer = timer;
            let mut body = result.into_stream();
            while let Some(bytes) = body.try_next().await? {
                metrics.update(&name, |m| m.response_bytes = Some(m.response_bytes.unwrap_or(0) + bytes.len() as u64));
                yield bytes;
            }
        }.boxed();
        Ok(GetResult {
            payload: GetResultPayload::Stream(payload),
            meta,
            range,
            attributes,
        })
    }
    async fn validate(
        &self,
        path: &Path,
        opts: &GetOptions,
        etag: &str,
    ) -> object_store::Result<(ObjectMeta, Attributes)> {
        let mut probe = opts.clone();
        probe.range = Some(GetRange::Bounded(0..1));
        let result = self.origin(path, probe).await?;
        if result.meta.e_tag.as_deref() != Some(etag) || result.range != (0..1) {
            return Err(invalid("source did not honor the pinned ETag/range"));
        }
        if opts
            .version
            .as_ref()
            .is_some_and(|version| result.meta.version.as_ref() != Some(version))
        {
            return Err(invalid("source did not honor the pinned object version"));
        }
        let meta = result.meta.clone();
        let attributes = result.attributes.clone();
        let mut stream = result.into_stream();
        let mut received = 0;
        while let Some(bytes) = stream.try_next().await? {
            received += bytes.len();
            if received > 1 {
                return Err(invalid("authorization probe exceeded its range"));
            }
        }
        if received != 1 {
            return Err(invalid("incomplete authorization probe"));
        }
        Ok((meta, attributes))
    }
    fn response(
        &self,
        data: Bytes,
        meta: ObjectMeta,
        attributes: Attributes,
        opts: &GetOptions,
    ) -> object_store::Result<GetResult> {
        let range = match &opts.range {
            Some(range) => range
                .as_range(meta.size)
                .map_err(|e| invalid(&e.to_string()))?,
            None => 0..meta.size,
        };
        if data.len() as u64 != meta.size {
            return Err(invalid("cached object size changed without an ETag change"));
        }
        let output = data.slice(range.start as usize..range.end as usize);
        self.metrics
            .update(&self.name, |m| m.cache_bytes += output.len() as u64);
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(async { Ok(output) }).boxed()),
            meta,
            range,
            attributes,
        })
    }
    async fn read(&self, path: &Path, opts: GetOptions) -> object_store::Result<GetResult> {
        let etag = match opts.if_match.as_deref() {
            Some(tag)
                if !opts.head
                    && opts.extensions.is_empty()
                    && tag != "*"
                    && !tag.contains(',')
                    && self.cache.max_object_bytes > 0 =>
            {
                tag.to_string()
            }
            _ => return self.origin(path, opts).await,
        };
        let Some(scope) = self.scope().await else {
            return self.origin(path, opts).await;
        };
        let key = CacheKey {
            scope,
            path: path.clone(),
            etag: etag.clone(),
            version: opts.version.clone(),
        };
        if let Some(data) = self.cache.get(&key) {
            // A real conditional GET is required even on cache hits. HEAD
            // cannot establish that GetObject (including KMS) is allowed.
            let (meta, attributes) = self.validate(path, &opts, &etag).await?;
            if self.scope().await != Some(scope) {
                return self.origin(path, opts).await;
            }
            self.metrics.update(&self.name, |m| m.cache_hits += 1);
            return self.response(data, meta, attributes, &opts);
        }
        self.metrics.update(&self.name, |m| m.cache_misses += 1);
        // The first request is the original ranged read. Large files and busy
        // fills incur neither an extra probe nor a head-of-line wait.
        let mut result = self.origin(path, opts.clone()).await?;
        let meta = result.meta.clone();
        let attributes = result.attributes.clone();
        if meta.size > self.cache.max_object_bytes as u64 || meta.size == 0 {
            return Ok(result);
        }
        let Ok(_fill) = self.cache.fill.try_lock() else {
            return Ok(result);
        };
        let (reservation, evictions) = self.cache.reserve(meta.size as usize);
        self.metrics
            .update(&self.name, |m| m.cache_evictions += evictions);
        let Some(reservation) = reservation else {
            return Ok(result);
        };
        if result.range != (0..meta.size) {
            // Drain without accumulating the first small ranged response to
            // preserve connection reuse and accurate consumed-byte metrics.
            let expected = result
                .range
                .end
                .checked_sub(result.range.start)
                .ok_or_else(|| invalid("invalid origin range"))?;
            if expected > meta.size {
                return Err(invalid("origin range exceeds object size"));
            }
            let mut received: u64 = 0;
            let mut body = result.into_stream();
            while let Some(bytes) = body.try_next().await? {
                received = received
                    .checked_add(bytes.len() as u64)
                    .ok_or_else(|| invalid("object size overflow"))?;
                if received > expected {
                    return Err(invalid("origin exceeded requested range"));
                }
            }
            if received != expected {
                return Err(invalid("incomplete origin range"));
            }
            let mut full = opts.clone();
            full.range = None;
            result = self.origin(path, full).await?;
        }
        if result.meta.e_tag.as_deref() != Some(etag.as_str())
            || result.meta.size != meta.size
            || result.range != (0..meta.size)
            || opts
                .version
                .as_ref()
                .is_some_and(|version| result.meta.version.as_ref() != Some(version))
        {
            return Err(invalid(
                "source changed or ignored the pinned whole-object request",
            ));
        }
        let mut owned = OwnedPayload {
            data: vec![0; meta.size as usize].into_boxed_slice(),
            _reservation: reservation,
        };
        let mut body = result.into_stream();
        let mut offset: usize = 0;
        while let Some(bytes) = body.try_next().await? {
            let end = offset
                .checked_add(bytes.len())
                .ok_or_else(|| invalid("object size overflow"))?;
            if end > owned.data.len() {
                return Err(invalid("object body exceeded declared size"));
            }
            owned.data[offset..end].copy_from_slice(&bytes);
            offset = end;
        }
        if offset != owned.data.len() {
            return Err(invalid("incomplete object body"));
        }
        let data = Bytes::from_owner(owned);
        // Do not populate the namespace of credentials that rotated mid-fetch.
        if self.scope().await == Some(scope) {
            self.cache.insert(key, data.clone());
        }
        self.response(data, meta, attributes, &opts)
    }
}
#[async_trait]
impl ObjectStore for CachedStore {
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
    async fn get_opts(&self, path: &Path, opts: GetOptions) -> object_store::Result<GetResult> {
        self.read(path, opts).await
    }
    fn list(&self, _: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        stream::once(async { Err(denied()) }).boxed()
    }
    async fn list_with_delimiter(&self, _: Option<&Path>) -> object_store::Result<ListResult> {
        Err(denied())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::{ObjectStoreExt, memory::InMemory};
    use std::{ops::Range, sync::atomic::AtomicBool};
    #[derive(Debug)]
    struct Source {
        inner: InMemory,
        denied: AtomicBool,
        probes: AtomicUsize,
        full: AtomicUsize,
        block_full: AtomicBool,
        bad_body: AtomicUsize,
        started: tokio::sync::Notify,
    }
    impl Source {
        fn new() -> Self {
            Self {
                inner: InMemory::new(),
                denied: AtomicBool::new(false),
                probes: AtomicUsize::new(0),
                full: AtomicUsize::new(0),
                block_full: AtomicBool::new(false),
                bad_body: AtomicUsize::new(0),
                started: tokio::sync::Notify::new(),
            }
        }
        async fn seed(&self, path: &Path, data: &'static str) -> String {
            self.inner.put(path, data.into()).await.unwrap();
            self.inner.head(path).await.unwrap().e_tag.unwrap()
        }
    }
    impl fmt::Display for Source {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "test source")
        }
    }
    #[async_trait]
    impl ObjectStore for Source {
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
        fn list(&self, _: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            stream::once(async { Err(denied()) }).boxed()
        }
        async fn list_with_delimiter(&self, _: Option<&Path>) -> object_store::Result<ListResult> {
            Err(denied())
        }
        async fn get_opts(&self, p: &Path, opts: GetOptions) -> object_store::Result<GetResult> {
            if !opts.head {
                if self.denied.load(Ordering::Acquire) {
                    return Err(denied());
                }
                if opts.range == Some(GetRange::Bounded(0..1)) {
                    self.probes.fetch_add(1, Ordering::Relaxed);
                } else if opts.range.is_none() {
                    self.full.fetch_add(1, Ordering::Relaxed);
                    self.started.notify_one();
                    if self.block_full.load(Ordering::Acquire) {
                        std::future::pending::<()>().await;
                    }
                }
            }
            let full = opts.range.is_none() && !opts.head;
            let mut result = self.inner.get_opts(p, opts).await?;
            if full {
                let bytes = match self.bad_body.load(Ordering::Acquire) {
                    1 => Some(Bytes::from_static(b"012345678")),
                    2 => Some(Bytes::from_static(b"X")),
                    _ => None,
                };
                if let Some(bytes) = bytes {
                    result.payload =
                        GetResultPayload::Stream(stream::once(async { Ok(bytes) }).boxed());
                }
            }
            Ok(result)
        }
    }
    fn wrapper(source: Arc<Source>, cache: Arc<ReadCache>, scope: u8) -> CachedStore {
        CachedStore {
            inner: source,
            cache,
            base_scope: [scope; 32],
            credentials: None,
            metrics: Metrics::default(),
            name: "test".into(),
        }
    }
    async fn read(
        store: &CachedStore,
        path: &Path,
        tag: &str,
        range: Option<Range<u64>>,
    ) -> object_store::Result<Bytes> {
        store
            .get_opts(
                path,
                GetOptions {
                    if_match: Some(tag.into()),
                    range: range.map(GetRange::Bounded),
                    ..Default::default()
                },
            )
            .await?
            .bytes()
            .await
    }
    #[tokio::test]
    async fn hits_require_real_get_permission_even_when_head_is_allowed() {
        let source = Arc::new(Source::new());
        let path = Path::from("one");
        let tag = source.seed(&path, "abcdefgh").await;
        let cache = Arc::new(ReadCache::new(16, 16));
        let store = wrapper(source.clone(), cache, 0);
        assert_eq!(read(&store, &path, &tag, Some(2..5)).await.unwrap(), "cde");
        assert_eq!(read(&store, &path, &tag, Some(0..2)).await.unwrap(), "ab");
        assert_eq!(source.full.load(Ordering::Acquire), 1);
        assert_eq!(source.probes.load(Ordering::Acquire), 1);
        source.denied.store(true, Ordering::Release);
        assert!(store.head(&path).await.is_ok());
        assert!(read(&store, &path, &tag, None).await.is_err());
        let metrics = store.metrics.snapshot();
        assert_eq!(metrics["test"].cache_hits, 1);
        assert_eq!(metrics["test"].response_bytes, Some(12));
    }
    #[tokio::test]
    async fn changed_version_fails_closed_and_new_pin_refills() {
        let source = Arc::new(Source::new());
        let path = Path::from("one");
        let old = source.seed(&path, "AAAA").await;
        let store = wrapper(source.clone(), Arc::new(ReadCache::new(16, 16)), 0);
        assert_eq!(read(&store, &path, &old, None).await.unwrap(), "AAAA");
        let new = source.seed(&path, "BBBB").await;
        assert!(read(&store, &path, &old, None).await.is_err());
        assert_eq!(read(&store, &path, &new, None).await.unwrap(), "BBBB");
    }
    #[tokio::test]
    async fn identical_paths_and_etags_cannot_cross_scopes() {
        let a = Arc::new(Source::new());
        let b = Arc::new(Source::new());
        let path = Path::from("one");
        let tag_a = a.seed(&path, "AAAA").await;
        let tag_b = b.seed(&path, "BBBB").await;
        assert_eq!(tag_a, tag_b);
        let cache = Arc::new(ReadCache::new(16, 16));
        assert_eq!(
            read(&wrapper(a, cache.clone(), 1), &path, &tag_a, None)
                .await
                .unwrap(),
            "AAAA"
        );
        assert_eq!(
            read(&wrapper(b, cache, 2), &path, &tag_b, None)
                .await
                .unwrap(),
            "BBBB"
        );
    }
    #[tokio::test]
    async fn eviction_counts_backing_allocations_until_last_slice_drops() {
        let source = Arc::new(Source::new());
        let cache = Arc::new(ReadCache::new(8, 4));
        let store = wrapper(source.clone(), cache.clone(), 0);
        let a = Path::from("a");
        let b = Path::from("b");
        let c = Path::from("c");
        let at = source.seed(&a, "AAAA").await;
        let bt = source.seed(&b, "BBBB").await;
        let ct = source.seed(&c, "CCCC").await;
        let held = read(&store, &a, &at, Some(0..1)).await.unwrap();
        drop(read(&store, &b, &bt, None).await.unwrap());
        assert_eq!(cache.resident_bytes(), 8);
        drop(read(&store, &c, &ct, None).await.unwrap());
        assert_eq!(cache.resident_bytes(), 8);
        assert_eq!(held, "A");
        drop(held);
        assert_eq!(cache.resident_bytes(), 4);
        assert_eq!(store.metrics.snapshot()["test"].cache_evictions, 2);
    }
    #[tokio::test]
    async fn concurrent_misses_have_one_fill_and_each_authorizes() {
        let source = Arc::new(Source::new());
        let path = Path::from("a");
        let tag = source.seed(&path, "ABCDEFGH").await;
        let store = wrapper(source.clone(), Arc::new(ReadCache::new(8, 8)), 0);
        let (a, b) = tokio::join!(
            read(&store, &path, &tag, None),
            read(&store, &path, &tag, None)
        );
        assert_eq!(a.unwrap(), b.unwrap());
        assert_eq!(source.full.load(Ordering::Acquire), 1);
        assert_eq!(source.probes.load(Ordering::Acquire), 1);
    }
    #[tokio::test]
    async fn cancellation_releases_fill_reservation_and_lock() {
        let source = Arc::new(Source::new());
        let path = Path::from("a");
        let tag = source.seed(&path, "ABCDEFGH").await;
        let cache = Arc::new(ReadCache::new(8, 8));
        let store = wrapper(source.clone(), cache.clone(), 0);
        source.block_full.store(true, Ordering::Release);
        let task = tokio::spawn({
            let store = store.clone();
            let path = path.clone();
            let tag = tag.clone();
            async move { read(&store, &path, &tag, Some(2..4)).await }
        });
        source.started.notified().await;
        assert_eq!(cache.resident_bytes(), 8);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(cache.resident_bytes(), 0);
        source.block_full.store(false, Ordering::Release);
        assert_eq!(read(&store, &path, &tag, None).await.unwrap(), "ABCDEFGH");
    }
    #[tokio::test]
    async fn a_busy_fill_does_not_block_other_live_range_reads() {
        let source = Arc::new(Source::new());
        let path = Path::from("a");
        let tag = source.seed(&path, "ABCDEFGH").await;
        let cache = Arc::new(ReadCache::new(8, 8));
        let store = wrapper(source.clone(), cache.clone(), 0);
        source.block_full.store(true, Ordering::Release);
        let task = tokio::spawn({
            let store = store.clone();
            let path = path.clone();
            let tag = tag.clone();
            async move { read(&store, &path, &tag, Some(2..4)).await }
        });
        source.started.notified().await;
        let second = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            read(&store, &path, &tag, Some(4..6)),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(second, "EF");
        assert_eq!(cache.resident_bytes(), 8);
        assert_eq!(source.full.load(Ordering::Acquire), 1);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(cache.resident_bytes(), 0);
    }
    #[tokio::test]
    async fn malformed_bodies_are_rejected_without_retaining_reserved_memory() {
        let source = Arc::new(Source::new());
        let path = Path::from("a");
        let tag = source.seed(&path, "ABCDEFGH").await;
        let cache = Arc::new(ReadCache::new(8, 8));
        let store = wrapper(source.clone(), cache.clone(), 0);
        for bad in [1, 2] {
            source.bad_body.store(bad, Ordering::Release);
            assert!(read(&store, &path, &tag, None).await.is_err());
            assert_eq!(cache.resident_bytes(), 0);
        }
    }
    #[derive(Debug)]
    struct RotatingCredential {
        token: AtomicUsize,
    }
    #[async_trait]
    impl object_store::CredentialProvider for RotatingCredential {
        type Credential = object_store::aws::AwsCredential;
        async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
            Ok(Arc::new(Self::Credential {
                key_id: "test-access-key".into(),
                secret_key: "test-secret".into(),
                token: Some(self.token.load(Ordering::Acquire).to_string()),
            }))
        }
    }
    #[tokio::test]
    async fn rotated_session_credentials_get_a_separate_cache_namespace() {
        let source = Arc::new(Source::new());
        let path = Path::from("a");
        let tag = source.seed(&path, "ABCDEFGH").await;
        let credentials = Arc::new(RotatingCredential {
            token: AtomicUsize::new(1),
        });
        let mut store = wrapper(source.clone(), Arc::new(ReadCache::new(16, 8)), 0);
        store.credentials = Some(credentials.clone());
        assert_eq!(read(&store, &path, &tag, None).await.unwrap(), "ABCDEFGH");
        assert_eq!(read(&store, &path, &tag, None).await.unwrap(), "ABCDEFGH");
        credentials.token.store(2, Ordering::Release);
        assert_eq!(read(&store, &path, &tag, None).await.unwrap(), "ABCDEFGH");
        assert_eq!(source.full.load(Ordering::Acquire), 2);
        assert_eq!(store.metrics.snapshot()["test"].cache_hits, 1);
        assert!(!format!("{store:?}").contains("test-secret"));
    }
    #[tokio::test]
    async fn oversized_and_unpinned_reads_are_never_retained() {
        let source = Arc::new(Source::new());
        let path = Path::from("a");
        let tag = source.seed(&path, "ABCDEFGH").await;
        let cache = Arc::new(ReadCache::new(4, 4));
        let store = wrapper(source.clone(), cache.clone(), 0);
        assert_eq!(read(&store, &path, &tag, Some(2..4)).await.unwrap(), "CD");
        assert_eq!(
            store.get(&path).await.unwrap().bytes().await.unwrap(),
            "ABCDEFGH"
        );
        assert_eq!(cache.resident_bytes(), 0);
        assert_eq!(source.probes.load(Ordering::Acquire), 0);
        assert_eq!(store.metrics.snapshot()["test"].requests, 2);
    }
    #[test]
    fn scope_hash_has_unambiguous_boundaries_and_no_debug_secrets() {
        assert_ne!(digest_parts(&[b"ab", b"c"]), digest_parts(&[b"a", b"bc"]));
        let source = Arc::new(Source::new());
        let store = wrapper(source, Arc::new(ReadCache::new(8, 8)), 0);
        assert_eq!(
            format!("{store:?}"),
            "AuthorizedReadCache { source: \"test\" }"
        );
    }
}
