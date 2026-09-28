//! Bounded, query-local read-ahead for successive small Parquet ranges.
//!
//! The first ranges are fetched exactly. Only a forward sequence of nearby reads
//! activates read-ahead, so a selective first row group does not fetch a large
//! prefix. This is an I/O buffer, not a reusable authorization or object cache:
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

#[derive(Default)]
struct AccessPattern {
    previous: Option<Range<u64>>,
    nearby_reads: usize,
    size: Option<u64>,
}

impl AccessPattern {
    fn observe(&mut self, range: &Range<u64>, gap: u64) {
        let nearby = self.previous.as_ref().is_some_and(|last| {
            range.start > last.start && range.start <= last.end.saturating_add(gap)
        });
        self.nearby_reads = if nearby {
            self.nearby_reads.saturating_add(1)
        } else {
            0
        };
        self.previous = Some(range.clone());
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
    window: usize,
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
        Self {
            budget,
            window,
            patterns: paths
                .map(|path| (path, tokio::sync::Mutex::new(AccessPattern::default())))
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
        let max_small = (self.window / 4).min(64 * 1024) as u64;
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
        pattern.observe(&request, max_small);
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
                    returned,
                    Bytes::copy_from_slice(&entry.bytes[start..end]),
                );
                buffers.push_back(entry);
                self.metrics
                    .update(&self.source, |m| m.read_ahead_hits += 1);
                return Ok(answer);
            }
        }
        let end = request
            .start
            .saturating_add(self.window as u64)
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
            let store = Arc::new(InMemory::new());
            let path = Path::from("orders.parquet");
            let bytes = Bytes::from((0..1024).map(|n| (n % 251) as u8).collect::<Vec<_>>());
            store.put(&path, bytes.clone().into()).await.unwrap();
            let tag = store.head(&path).await.unwrap().e_tag.unwrap();
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(limit));
            let budget = Arc::new(ReadAheadBudget::new(pool.clone(), limit));
            let reader = Arc::new(ReadAhead::new(
                std::iter::once(path.clone()),
                budget.clone(),
                64,
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
}
