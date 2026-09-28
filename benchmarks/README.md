# Reproduce the local comparison

The baseline uses Datatailr QE `0.1.0-pre300-arm64`, Spark 3.5.6 and this Neura checkout. QE was identified from the locally available image; the user's meaning of “QE” has not been independently confirmed. An accessible QE image is required; no substitute is silently used. The provided comparison setup targets arm64. For other architectures, pin corresponding QE/JRE images and record that change as a separate experiment.

Prerequisites: Docker, Rust 1.98.1, Python 3, `uv`, and approximately 20 GB of free build space. Docker needs enough RAM to run the three 2 GiB engine containers and two 512 MiB sources. Build the toolchain image first if using the standard Docker build instead of the low-disk path below.

```sh
docker compose -f deploy/compose.test.yml up -d --wait
NEURA_FIXTURE_ROWS=1000000 uv run --with pyarrow==21.0.0 --with boto3==1.40.40 scripts/seed.py
source scripts/test-env.sh
bash scripts/build-low-disk.sh
python3 benchmarks/prepare.py
uv run --with 'psycopg[binary]==3.2.10' benchmarks/run.py
```

The setup refuses to replace an existing benchmark container. Remove only the named `neura-bench-neura`, `neura-bench-qe` and `neura-bench-spark` containers when intentionally starting a new experiment. The harness restarts only these containers. Do not reseed the fixture while queries are running: Neura deliberately rejects changed object ETags.

All engines have the same hard **2 CPU, 2 GiB RAM, no extra swap** limits. Neura/QE tracked operator pools and Spark's JVM driver heap are each 512 MiB; these internal accounting schemes are not equivalent, so cgroup measurements are reported separately. Engines use two execution threads/partitions. The same restricted PostgreSQL role (including RLS) and read-only S3 credential access the same immutable Parquet files. The Spark adapter substitutes the two catalog identifiers with temporary-view names; SQL relational operations are otherwise identical.

Each of five workloads has one process-cold sample after an engine restart, then a successful full priming query and five warm samples. Source processes, OS caches and storage caches remain running. **These are not storage-cold measurements.** Readiness checks are excluded, so cold latency excludes engine startup. Result/data caching is disabled in QE; Spark tables are not explicitly cached and Neura has no result cache. Normal metadata, connection and OS caches still exist.

The harness checks every result against a separate Python oracle, preserves failures and records latency including connection/authentication and full result fetching. PostgreSQL-wire clients are used for Neura/QE; Spark uses a local HTTP adapter around its persistent driver. Protocol overhead is part of measured latency and differs. Throughput measures ten warm federated queries at concurrency two. This is a small synthetic workload, not a saturation or distributed-scaling study. Run more repetitions and larger/varied datasets for production decisions.

Neura client connections use SCRAM authentication. The local QE endpoint has no client password configured, and the local Spark HTTP adapter has no authentication. Source credentials and permissions remain identical. In particular, short-query latency is not an isolated execution-time comparison: it includes these different authentication paths. Cube normally reuses pooled connections. The fixture's 1,024-row Parquet groups deliberately exercise pruning, but full scans amplify small-range request overhead; it is not representative of every production Parquet layout.

Neura uses its default `parquet_row_filter_pushdown=false`: file/row-group pruning remains enabled, while row-level late materialization is opt-in. Diagnostic runs showed that row-level filtering increased range requests for this low-selectivity join. The published comparison uses the final source/image identity, not those interrupted diagnostic runs.

Memory is Linux cgroup `memory.current` and `memory.peak`. Peak is cumulative since the last container restart, includes startup and filesystem cache, and is **not per-query RSS**. The harness refuses unequal limits and records image identities. Raw JSON goes to `results/comparison.json`, with wrong results and errors retained. It exits unsuccessfully if any correctness check fails. Published measurements and limitations are in `docs/VALIDATION.md` and `docs/benchmark-results/`.
