//! Bounded, query-local read-ahead for successive small Parquet ranges.
//!
//! The first ranges are fetched exactly. Only a forward sequence of nearby reads
//! activates read-ahead, so a selective first row group does not fetch a large
//! prefix. Windows start at no more than 256 KiB and grow toward the configured
//! maximum only after sustained forward hits consume most of the current span.
//! This is an I/O buffer, not a reusable authorization or object cache:
//! the owner is a single query's manifest-pinned object store.

use bytes::{Bytes, BytesMut};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use futures::{StreamExt, stream};
use object_store::{
    Attributes, GetOptions, GetRange, GetResult, GetResultPayload, ObjectMeta, path::Path,
};
use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    ops::Range,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use crate::metrics::Metrics;

/// One budget shared by every manifest-backed S3 source in a query. Buffers also
/// reserve the engine memory pool; exhausted budgets fall back to exact reads.
pub struct ReadAheadBudget {
    pool: Arc<dyn MemoryPool>,
    limit: usize,
    used: AtomicUsize,
}

impl ReadAheadBudget {
    pub fn new(pool: Arc<dyn MemoryPool>, limit: usize) -> Self {
        Self {
            pool,
            limit,
            used: AtomicUsize::new(0),
        }
    }

    fn reserve(self: &Arc<Self>, size: usize) -> Option<BufferReservation> {
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(size).filter(|next| *next <= self.limit)
            })
            .ok()?;
        let memory = MemoryConsumer::new("S3 query read-ahead").register(&self.pool);
        if memory.try_grow(size).is_err() {
            self.used.fetch_sub(size, Ordering::AcqRel);
            return None;
        }
        Some(BufferReservation {
            budget: self.clone(),
            memory,
        })
    }
}

struct BufferReservation {
    budget: Arc<ReadAheadBudget>,
    memory: MemoryReservation,
}

impl Drop for BufferReservation {
    fn drop(&mut self) {
        self.budget
            .used
            .fetch_sub(self.memory.size(), Ordering::AcqRel);
    }
}

struct AccessPattern {
    previous: Option<Range<u64>>,
    nearby_reads: usize,
    size: Option<u64>,
    window: usize,
    progress: Option<WindowProgress>,
}

struct WindowProgress {
    range: Range<u64>,
    hits: usize,
    furthest_end: u64,
}

impl AccessPattern {
    fn new(window: usize) -> Self {
        Self {
            previous: None,
            nearby_reads: 0,
            size: None,
            window,
            progress: None,
        }
    }

    fn observe(&mut self, range: &Range<u64>, gap: u64, initial_window: usize) -> bool {
        let nearby = self.previous.as_ref().is_some_and(|last| {
            range.start > last.start && range.start <= last.end.saturating_add(gap)
        });
        if nearby {
            self.nearby_reads = self.nearby_reads.saturating_add(1);
        } else if self.previous.as_ref() != Some(range) {
            self.nearby_reads = 0;
            self.window = initial_window;
            self.progress = None;
        }
        self.previous = Some(range.clone());
        nearby
    }

    fn hit(&mut self, buffer: &Range<u64>, returned_end: u64) {
        if let Some(progress) = &mut self.progress
            && progress.range == *buffer
        {
            progress.hits = progress.hits.saturating_add(1);
            progress.furthest_end = progress.furthest_end.max(returned_end);
        }
    }

    fn next_window(&self, request: &Range<u64>, gap: u64, maximum: usize) -> usize {
        let consumed = self.progress.as_ref().is_some_and(|progress| {
            let span = progress.range.end - progress.range.start;
            let extent = progress.furthest_end - progress.range.start;
            progress.hits >= 8
                && extent >= span - span / 4
                && request.start >= progress.range.end.saturating_sub(gap)
                && request.start <= progress.range.end.saturating_add(gap)
        });
        if consumed {
            self.window.saturating_mul(2).min(maximum)
        } else {
            self.window
        }
    }
}

struct Buffer {
    path: Path,
    range: Range<u64>,
    bytes: Bytes,
    meta: ObjectMeta,
    attributes: Attributes,
    _reservation: BufferReservation,
}

pub(super) struct ReadAhead {
    budget: Arc<ReadAheadBudget>,
    initial_window: usize,
    maximum_window: usize,
    patterns: HashMap<Path, tokio::sync::Mutex<AccessPattern>>,
    buffers: Mutex<VecDeque<Buffer>>,
    metrics: Metrics,
    source: String,
}

fn result(meta: ObjectMeta, attributes: Attributes, range: Range<u64>, bytes: Bytes) -> GetResult {
    GetResult {
        meta,
        attributes,
        range,
        payload: GetResultPayload::Stream(stream::once(async { Ok(bytes) }).boxed()),
    }
}

fn invalid_response(message: &str) -> object_store::Error {
    object_store::Error::Generic {
        store: "Neura S3 read-ahead",
        source: message.to_string().into(),
    }
}

impl ReadAhead {
    pub(super) fn new(
        paths: impl Iterator<Item = Path>,
        budget: Arc<ReadAheadBudget>,
        window: usize,
        metrics: Metrics,
        source: String,
    ) -> Self {
        let initial_window = window.min(256 * 1024).min(budget.limit);
        // Growth leaves room for two active objects. A deliberately small
        // configured window may still use the entire budget, as before.
        let maximum_window = window.min((budget.limit / 2).max(initial_window));
        Self {
            budget,
            initial_window,
            maximum_window,
            patterns: paths
                .map(|path| {
                    (
                        path,
                        tokio::sync::Mutex::new(AccessPattern::new(initial_window)),
                    )
                })
                .collect(),
            buffers: Mutex::new(VecDeque::new()),
            metrics,
            source,
        }
    }

    pub(super) async fn get<F, Fut>(
        &self,
        path: &Path,
        mut opts: GetOptions,
        fetch: F,
    ) -> object_store::Result<GetResult>
    where
        F: FnOnce(GetOptions) -> Fut,
        Fut: Future<Output = object_store::Result<GetResult>>,
    {
        // Preserve unusual caller preconditions/version requests verbatim. The
        // outer store has already imposed the manifest's non-wildcard ETag.
        let request = match &opts.range {
            Some(GetRange::Bounded(range)) if range.start < range.end => range.clone(),
            _ => return fetch(opts).await,
        };
        let max_small = (self.initial_window / 4).min(64 * 1024) as u64;
        if opts.head
            || opts.version.is_some()
            || opts.if_none_match.is_some()
            || opts.if_modified_since.is_some()
            || opts.if_unmodified_since.is_some()
            || !opts.extensions.is_empty()
            || request.end - request.start > max_small
            || !opts
                .if_match
                .as_ref()
                .is_some_and(|tag| !tag.is_empty() && tag != "*")
        {
            return fetch(opts).await;
        }
        let Some(pattern) = self.patterns.get(path) else {
            return fetch(opts).await;
        };
        // Serialize same-object small reads only. Different objects retain their
        // concurrency, and cancellation drops this guard and any reservation.
        let mut pattern = pattern.lock().await;
        let forward = pattern.observe(&request, max_small, self.initial_window);
        {
            let mut buffers = self.buffers.lock().unwrap();
            if let Some(index) = buffers.iter().position(|entry| {
                entry.path == *path
                    && entry.range.start <= request.start
                    && entry.range.end >= request.end.min(entry.meta.size)
                    && request.start < entry.meta.size
            }) {
                let entry = buffers.remove(index).unwrap();
                let returned = request.start..request.end.min(entry.meta.size);
                let start = (returned.start - entry.range.start) as usize;
                let end = (returned.end - entry.range.start) as usize;
                // Copy just the requested bytes: downstream readers may retain a
                // slice, which must not keep an evicted read-ahead window alive.
                let answer = result(
                    entry.meta.clone(),
                    entry.attributes.clone(),
                    returned.clone(),
                    Bytes::copy_from_slice(&entry.bytes[start..end]),
                );
                if forward {
                    pattern.hit(&entry.range, returned.end);
                }
                buffers.push_back(entry);
                self.metrics
                    .update(&self.source, |m| m.read_ahead_hits += 1);
                return Ok(answer);
            }
        }
        let window = pattern.next_window(&request, max_small, self.maximum_window);
        let end = request
            .start
            .saturating_add(window as u64)
            .min(pattern.size.unwrap_or(request.end));
        if pattern.nearby_reads < 2 || end <= request.end {
            let answer = fetch(opts).await?;
            pattern.size = Some(answer.meta.size);
            return Ok(answer);
        }
        let size = (end - request.start) as usize;
        let reservation = {
            let mut buffers = self.buffers.lock().unwrap();
            loop {
                if let Some(reservation) = self.budget.reserve(size) {
                    break Some(reservation);
                }
                if buffers.pop_front().is_none() {
                    break None;
                }
            }
        };
        let Some(reservation) = reservation else {
            return fetch(opts).await;
        };
        opts.range = Some(GetRange::Bounded(request.start..end));
        let fetched = fetch(opts).await?;
        if fetched.range.start != request.start
            || fetched.range.end > end
            || fetched.range.end < request.end.min(fetched.meta.size)
        {
            return Err(invalid_response(
                "object store returned an inconsistent prefetched range",
            ));
        }
        let meta = fetched.meta.clone();
        let attributes = fetched.attributes.clone();
        let range = fetched.range.clone();
        let mut bytes = BytesMut::with_capacity(size);
        let mut stream = fetched.into_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            if chunk.len() > size - bytes.len() {
                return Err(invalid_response(
                    "object store exceeded the bounded read-ahead response",
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.len() as u64 != range.end - range.start {
            return Err(invalid_response(
                "object store returned a truncated prefetched range",
            ));
        }
        pattern.size = Some(meta.size);
        let returned = request.start..request.end.min(meta.size);
        pattern.window = window;
        pattern.progress = Some(WindowProgress {
            range: range.clone(),
            hits: 0,
            furthest_end: returned.end,
        });
        let answer = result(
            meta.clone(),
            attributes.clone(),
            returned.clone(),
            Bytes::copy_from_slice(&bytes[..(returned.end - returned.start) as usize]),
        );
        self.metrics.update(&self.source, |m| {
            m.read_ahead_bytes += bytes.len() as u64 - (returned.end - returned.start)
        });
        self.buffers.lock().unwrap().push_back(Buffer {
            path: path.clone(),
            range,
            bytes: bytes.freeze(),
            meta,
            attributes,
            _reservation: reservation,
        });
        Ok(answer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory};

    struct Fixture {
        store: Arc<InMemory>,
        path: Path,
        tag: String,
        bytes: Bytes,
        budget: Arc<ReadAheadBudget>,
        pool: Arc<dyn MemoryPool>,
        reader: Arc<ReadAhead>,
        requests: Arc<Mutex<Vec<Range<u64>>>>,
    }

    impl Fixture {
        async fn new(limit: usize) -> Self {
            Self::with_window(limit, 64, 1024).await
        }

        async fn with_window(limit: usize, window: usize, length: usize) -> Self {
            let store = Arc::new(InMemory::new());
            let path = Path::from("orders.parquet");
            let bytes = Bytes::from((0..length).map(|n| (n % 251) as u8).collect::<Vec<_>>());
            store.put(&path, bytes.clone().into()).await.unwrap();
            let tag = store.head(&path).await.unwrap().e_tag.unwrap();
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(limit));
            let budget = Arc::new(ReadAheadBudget::new(pool.clone(), limit));
            let reader = Arc::new(ReadAhead::new(
                std::iter::once(path.clone()),
                budget.clone(),
                window,
                Metrics::default(),
                "s3".into(),
            ));
            Self {
                store,
                path,
                tag,
                bytes,
                budget,
                pool,
                reader,
                requests: Arc::default(),
            }
        }

        fn opts(&self, range: Range<u64>) -> GetOptions {
            GetOptions {
                range: Some(GetRange::Bounded(range)),
                if_match: Some(self.tag.clone()),
                ..Default::default()
            }
        }

        async fn get(&self, range: Range<u64>) -> object_store::Result<Bytes> {
            self.reader
                .get(&self.path, self.opts(range), |opts| async {
                    let Some(GetRange::Bounded(range)) = &opts.range else {
                        panic!("expected bounded request")
                    };
                    self.requests.lock().unwrap().push(range.clone());
                    assert_eq!(opts.if_match.as_deref(), Some(self.tag.as_str()));
                    self.store.get_opts(&self.path, opts).await
                })
                .await?
                .bytes()
                .await
        }
    }

    #[tokio::test]
    async fn nearby_ranges_coalesce_without_changing_requested_bytes() {
        let fixture = Fixture::new(128).await;
        for start in (0..64).step_by(4) {
            let bytes = fixture.get(start..start + 4).await.unwrap();
            assert_eq!(
                bytes,
                fixture.bytes.slice(start as usize..start as usize + 4)
            );
        }
        assert_eq!(*fixture.requests.lock().unwrap(), vec![0..4, 4..8, 8..72]);
        let metrics = fixture.reader.metrics.snapshot();
        assert_eq!(metrics["s3"].read_ahead_hits, 13);
        assert_eq!(metrics["s3"].read_ahead_bytes, 60);
        assert_eq!(fixture.pool.reserved(), 64);
    }

    #[tokio::test]
    async fn isolated_and_large_ranges_do_not_speculate() {
        let fixture = Fixture::new(128).await;
        for range in [0..4, 200..204, 600..604, 700..780] {
            assert_eq!(
                fixture.get(range.clone()).await.unwrap(),
                fixture
                    .bytes
                    .slice(range.start as usize..range.end as usize)
            );
        }
        assert_eq!(
            *fixture.requests.lock().unwrap(),
            vec![0..4, 200..204, 600..604, 700..780]
        );
        assert_eq!(fixture.budget.used.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn eviction_and_copied_results_release_the_entire_reservation() {
        let fixture = Fixture::new(64).await;
        let mut retained = vec![];
        for start in (0..256).step_by(4) {
            retained.push(fixture.get(start..start + 4).await.unwrap());
            assert!(fixture.budget.used.load(Ordering::Acquire) <= 64);
            assert!(fixture.pool.reserved() <= 64);
        }
        let Fixture {
            reader,
            pool,
            budget,
            ..
        } = fixture;
        drop(reader);
        assert_eq!(budget.used.load(Ordering::Acquire), 0);
        assert_eq!(pool.reserved(), 0);
        assert_eq!(retained[0].as_ref(), &[0, 1, 2, 3]);
        assert_eq!(retained[63].as_ref(), &[1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn pool_pressure_falls_back_to_exact_reads() {
        let fixture = Fixture::new(64).await;
        let reservation = MemoryConsumer::new("query operator").register(&fixture.pool);
        reservation.try_grow(32).unwrap();
        for start in [0, 4, 8, 12] {
            fixture.get(start..start + 4).await.unwrap();
        }
        assert_eq!(
            *fixture.requests.lock().unwrap(),
            vec![0..4, 4..8, 8..12, 12..16]
        );
        assert_eq!(fixture.pool.reserved(), 32);
        assert_eq!(fixture.budget.used.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn same_object_concurrent_reads_share_one_prefetch() {
        let fixture = Fixture::new(128).await;
        fixture.get(0..4).await.unwrap();
        fixture.get(4..8).await.unwrap();
        let (left, right) = tokio::join!(fixture.get(8..12), fixture.get(8..12));
        assert_eq!(left.unwrap(), right.unwrap());
        assert_eq!(fixture.requests.lock().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn changed_etag_fails_before_prefetch_and_leaves_no_reservation() {
        let fixture = Fixture::new(128).await;
        fixture.get(0..4).await.unwrap();
        fixture.get(4..8).await.unwrap();
        fixture
            .store
            .put(&fixture.path, Bytes::from_static(b"replacement").into())
            .await
            .unwrap();
        assert!(matches!(
            fixture.get(8..12).await,
            Err(object_store::Error::Precondition { .. })
        ));
        assert_eq!(fixture.pool.reserved(), 0);
        assert_eq!(fixture.budget.used.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn cancellation_releases_in_flight_reservation() {
        let fixture = Fixture::new(128).await;
        fixture.get(0..4).await.unwrap();
        fixture.get(4..8).await.unwrap();
        let reader = fixture.reader.clone();
        let path = fixture.path.clone();
        let opts = fixture.opts(8..12);
        let started = Arc::new(tokio::sync::Notify::new());
        let signal = started.clone();
        let task = tokio::spawn(async move {
            reader
                .get(&path, opts, |_| async move {
                    signal.notify_one();
                    std::future::pending::<object_store::Result<GetResult>>().await
                })
                .await
        });
        started.notified().await;
        assert_eq!(fixture.pool.reserved(), 64);
        task.abort();
        let _ = task.await;
        assert_eq!(fixture.pool.reserved(), 0);
        assert_eq!(fixture.budget.used.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn oversized_origin_response_is_rejected_before_growing_buffer() {
        let fixture = Fixture::new(128).await;
        fixture.get(0..4).await.unwrap();
        fixture.get(4..8).await.unwrap();
        let meta = fixture.store.head(&fixture.path).await.unwrap();
        let response = fixture
            .reader
            .get(&fixture.path, fixture.opts(8..12), |_| async {
                Ok(result(
                    meta,
                    Attributes::default(),
                    8..72,
                    Bytes::from(vec![0; 65]),
                ))
            })
            .await;
        assert!(response.is_err());
        assert_eq!(fixture.pool.reserved(), 0);
    }

    #[tokio::test]
    async fn special_preconditions_always_reach_origin() {
        let fixture = Fixture::new(128).await;
        for start in [0, 4, 8] {
            fixture.get(start..start + 4).await.unwrap();
        }
        let mut opts = fixture.opts(12..16);
        opts.if_none_match = Some(fixture.tag.clone());
        let response = fixture
            .reader
            .get(&fixture.path, opts, |opts| async {
                fixture.store.get_opts(&fixture.path, opts).await
            })
            .await;
        assert!(matches!(
            response,
            Err(object_store::Error::NotModified { .. })
        ));
    }

    #[tokio::test]
    async fn sustained_forward_consumption_grows_windows_within_query_budget() {
        let fixture = Fixture::with_window(2 * 1024 * 1024, 1024 * 1024, 3 * 1024 * 1024).await;
        for start in (0..800 * 1024).step_by(4096) {
            assert_eq!(
                fixture.get(start..start + 4096).await.unwrap(),
                fixture.bytes.slice(start as usize..start as usize + 4096)
            );
            assert!(fixture.pool.reserved() <= 2 * 1024 * 1024);
            assert!(fixture.budget.used.load(Ordering::Acquire) <= 2 * 1024 * 1024);
        }
        assert_eq!(
            fixture
                .requests
                .lock()
                .unwrap()
                .iter()
                .map(|range| range.end - range.start)
                .collect::<Vec<_>>(),
            vec![4096, 4096, 256 * 1024, 512 * 1024, 1024 * 1024]
        );
        assert_eq!(fixture.pool.reserved(), 1792 * 1024);
    }

    #[tokio::test]
    async fn selective_prefix_and_sparse_hits_do_not_grow_windows() {
        let fixture = Fixture::with_window(2 * 1024 * 1024, 1024 * 1024, 3 * 1024 * 1024).await;
        // Many hits in a small prefix do not demonstrate consumption of 75%.
        for start in (0..64 * 1024).step_by(4096) {
            fixture.get(start..start + 4096).await.unwrap();
        }
        let pattern = fixture.reader.patterns[&fixture.path].lock().await;
        assert_eq!(pattern.window, 256 * 1024);
        assert!(pattern.progress.as_ref().unwrap().hits >= 8);
        assert_eq!(
            pattern.next_window(&(264 * 1024..268 * 1024), 64 * 1024, 1024 * 1024),
            256 * 1024
        );
        drop(pattern);
        assert_eq!(fixture.requests.lock().unwrap().len(), 3);

        // Covering most of a window with only four hits is insufficient too.
        let sparse = Fixture::with_window(2 * 1024 * 1024, 1024 * 1024, 3 * 1024 * 1024).await;
        for start in [0, 4, 8, 68, 128, 188, 248, 264].map(|kib| kib * 1024) {
            sparse.get(start..start + 4096).await.unwrap();
        }
        assert_eq!(
            sparse
                .requests
                .lock()
                .unwrap()
                .iter()
                .map(|range| range.end - range.start)
                .collect::<Vec<_>>(),
            vec![4096, 4096, 256 * 1024, 256 * 1024]
        );
    }

    #[tokio::test]
    async fn random_and_backward_access_reset_growth_and_objects_are_independent() {
        let mut fixture = Fixture::with_window(2 * 1024 * 1024, 1024 * 1024, 3 * 1024 * 1024).await;
        let other = Path::from("another-object.parquet");
        fixture
            .store
            .put(&other, fixture.bytes.clone().into())
            .await
            .unwrap();
        let tag = fixture.store.head(&other).await.unwrap().e_tag.unwrap();
        fixture.reader = Arc::new(ReadAhead::new(
            [fixture.path.clone(), other.clone()].into_iter(),
            fixture.budget.clone(),
            1024 * 1024,
            Metrics::default(),
            "s3".into(),
        ));
        for start in (0..800 * 1024).step_by(4096) {
            fixture.get(start..start + 4096).await.unwrap();
        }
        assert_eq!(
            fixture.reader.patterns[&fixture.path].lock().await.window,
            1024 * 1024
        );
        fixture.get(0..4096).await.unwrap();
        assert_eq!(
            fixture.reader.patterns[&fixture.path].lock().await.window,
            256 * 1024
        );
        for start in [2400, 2404, 2408].map(|kib| kib * 1024) {
            fixture.get(start..start + 4096).await.unwrap();
        }
        {
            let requests = fixture.requests.lock().unwrap();
            let final_request = requests.last().unwrap();
            assert_eq!(final_request.end - final_request.start, 256 * 1024);
        }

        // Another manifest object starts conservatively even with the same
        // query budget and a previously-promoted object in this store.
        let requests = Mutex::new(vec![]);
        for start in [0, 4096, 8192] {
            let opts = GetOptions {
                range: Some(GetRange::Bounded(start..start + 4096)),
                if_match: Some(tag.clone()),
                ..Default::default()
            };
            fixture
                .reader
                .get(&other, opts, |opts| async {
                    let Some(GetRange::Bounded(range)) = &opts.range else {
                        panic!("bounded request")
                    };
                    requests.lock().unwrap().push(range.clone());
                    fixture.store.get_opts(&other, opts).await
                })
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
        }
        assert_eq!(
            requests.lock().unwrap().last().unwrap().end - 8192,
            256 * 1024
        );
        assert!(fixture.pool.reserved() <= 2 * 1024 * 1024);
    }

    #[tokio::test]
    async fn grown_window_reservation_is_released_on_cancellation_or_origin_error() {
        let fixture = Fixture::with_window(2 * 1024 * 1024, 1024 * 1024, 3 * 1024 * 1024).await;
        for start in (0..776 * 1024).step_by(4096) {
            fixture.get(start..start + 4096).await.unwrap();
        }
        let baseline = fixture.pool.reserved();
        assert_eq!(baseline, 768 * 1024);
        let reader = fixture.reader.clone();
        let path = fixture.path.clone();
        let opts = fixture.opts(776 * 1024..780 * 1024);
        let started = Arc::new(tokio::sync::Notify::new());
        let signal = started.clone();
        let task = tokio::spawn(async move {
            reader
                .get(&path, opts, |opts| async move {
                    let Some(GetRange::Bounded(range)) = opts.range else {
                        panic!("bounded request")
                    };
                    assert_eq!(range.end - range.start, 1024 * 1024);
                    signal.notify_one();
                    std::future::pending::<object_store::Result<GetResult>>().await
                })
                .await
        });
        started.notified().await;
        assert_eq!(fixture.pool.reserved(), baseline + 1024 * 1024);
        task.abort();
        let _ = task.await;
        assert_eq!(fixture.pool.reserved(), baseline);
        assert_eq!(fixture.budget.used.load(Ordering::Acquire), baseline);
        fixture
            .store
            .put(&fixture.path, "replacement".into())
            .await
            .unwrap();
        assert!(matches!(
            fixture.get(776 * 1024..780 * 1024).await,
            Err(object_store::Error::Precondition { .. })
        ));
        assert_eq!(fixture.pool.reserved(), baseline);
        assert_eq!(fixture.budget.used.load(Ordering::Acquire), baseline);
    }

    #[test]
    fn configured_maximum_and_budget_cap_initial_size_and_growth() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(2 * 1024 * 1024));
        for (budget, maximum, initial, expected_maximum) in [
            (2 * 1024 * 1024, 4 * 1024 * 1024, 256 * 1024, 1024 * 1024),
            (256 * 1024, 1024 * 1024, 256 * 1024, 256 * 1024),
            (64, 64, 64, 64),
            (32, 64, 32, 32),
            (0, 64, 0, 0),
        ] {
            let reader = ReadAhead::new(
                std::iter::empty(),
                Arc::new(ReadAheadBudget::new(pool.clone(), budget)),
                maximum,
                Metrics::default(),
                "s3".into(),
            );
            assert_eq!(reader.initial_window, initial);
            assert_eq!(reader.maximum_window, expected_maximum);
        }
    }
}
