# Same-host Neura / Query Engine comparison

The measured Neura Rust source is commit `5bcc99c`; the scripts in this directory
were added for the comparison. Exact Rust, harness, fixture and configuration
hashes are recorded. This is a native Linux x86_64 run on the user's Hetzner-QE
host, not a comparison between a laptop and a remote service.

The primary QE image must match the existing `phase1-qe` service. The script
asserts that match by image ID. A second profile uses the newer `pre341` image
already available on the same host. The existing services are not reconfigured,
stopped or restarted. Their process IDs, start times, image IDs and restart counts
are checked before/after, with unrelated service names hashed in published data.

## Prepare in a separate directory

Requirements: SSH access to the selected host, Docker, Python 3, GCC with static
libc support, the two actual QE images, and enough spare memory/disk for a native
Rust build. The published preparation uses a 4 CPU / 5 GiB build container; build
limits are independent of the later 2 CPU / 2 GiB benchmark-engine limits.

From the Neura checkout containing these scripts, transfer the source and the
exact four Parquet files produced by the [earlier fixture setup](COMPARE.md).
Record the actual checkout revision as the source commit for the new run.

```sh
git rev-parse HEAD
git archive HEAD | ssh -T -o ServerAliveInterval=15 Hetzner-QE \
  'mkdir -m 700 /opt/neura-benchmark-20260928 && tar -xf - -C /opt/neura-benchmark-20260928'
COPYFILE_DISABLE=1 tar -cf - data/compare/manifest.json data/compare/neura.toml \
  data/compare/small-0.parquet data/compare/small-1.parquet \
  data/compare/large-0.parquet data/compare/large-1.parquet | \
  ssh -T -o ServerAliveInterval=15 Hetzner-QE \
    'tar -xf - -C /opt/neura-benchmark-20260928'
```

The directory/network/container namespace is deliberately explicit. Preparation
refuses existing fixture state or containers. Reuse the retained stopped profiles
for another measurement; do not replace fixtures while any query is running.
Do not copy environment files, production credentials or CompanyBrain files.

On the selected host, inside `/opt/neura-benchmark-20260928`:

```sh
mkdir -p results target/linux-cargo target/linux-build
docker build -f deploy/Dockerfile.toolchain -t neura-hetzner-toolchain:1.98.1 .
docker run --rm --name neura-hetzner-build-20260928 --cpus 4 --memory 5g --memory-swap 5g \
  -v "$PWD:/work" -v "$PWD/target/linux-cargo:/cargo-cache" \
  -v "$PWD/target/linux-build:/build-target" -w /work \
  -e RUSTUP_TOOLCHAIN=1.98.1 -e CARGO_HOME=/cargo-cache -e CARGO_TARGET_DIR=/build-target \
  neura-hetzner-toolchain:1.98.1 cargo build --release --locked -j 3
docker build -f deploy/Dockerfile.prebuilt -t neura:hetzner-5bcc99c .

# Install fixture tooling in this workspace, without changing host Python packages.
docker run --rm --cpus 1 --memory 256m \
  -v "$PWD/target/bench-python:/vendor" python:3.12-slim \
  pip install --disable-pip-version-check --target /vendor boto3==1.40.40
PYTHONPATH=target/bench-python python3 benchmarks/prepare_qe_host.py fixtures
python3 benchmarks/prepare_qe_host.py engines
```

Fixtures use dedicated PostgreSQL 17.6 and SeaweedFS 3.94 containers. They are not
the existing host databases or object store. PostgreSQL has 1 CPU / 512 MiB, and
S3 has 1 CPU / 1 GiB with `GOMEMLIMIT=512MiB`; neither has extra swap. An initial
512 MiB S3 fixture was OOM-killed, invalidating that timing attempt. Its raw
observations and the fixture recovery are retained separately; engine limits
were unchanged. Transferred Parquet hashes must match before upload; ETags are read back.
The small/large layouts have identical logical rows. A separate local directory
contains hard links to the same large-layout files for local operator tests.

Every engine has 2 CPU / 2 GiB / no extra swap. Each shared pool is configured at
512 MiB; accounting differs. QE additionally has its default 1 GiB per-query
estimate guard. Neura has no directly equivalent separate estimate guard.
Data/result caches and QE auto-optimized result reuse are disabled. Neura retains
its default bounded read-ahead and admission-aware partition targets.

The preparation creates only localhost listener ports (58402 for Neura, 58413 and
58414 for QE HTTP, 58403/58404 for their PG listeners, and 58400 for the fixture S3
endpoint). Source credentials are deliberately restricted fixture identities.
API/bootstrap credentials are generated into a mode-0600 file under the private
workspace; the runner never publishes them. QE uses standalone JWT authentication;
its token-auth REST mode does not accept SQL queries. Neura uses its static bearer
token. QE login occurs during readiness, outside query timings.

## Run

Wait for compilation to finish. All benchmark-engine copies must be stopped
before starting the runner. Use the actual Rust checkout revision recorded above
for `--source-commit`; the published experiment used the value below.

```sh
python3 benchmarks/compare_qe_host.py \
  --source-commit 5bcc99c820e614532defb4bfec13117f1c97b14f \
  --smoke --output results/hetzner-qe-smoke.json
python3 benchmarks/compare_qe_host.py \
  --source-commit 5bcc99c820e614532defb4bfec13117f1c97b14f \
  --repeats 7 --throughput-queries 30 --concurrency 2 \
  --output results/hetzner-qe.json
```

**A nonzero exit caused by a rejected query is a benchmark result.** The runner
preserves errors/results and stops only the benchmark copy it started. Do not
turn a resource failure into a fast latency number. The million-group subquery
fails QE's 1 GiB estimate guard; this remains in the main report. Seven original
workloads and the additional distinct-count control can still be assessed from
their validated observations. A separate initial smoke control also records QE
rejecting the subquery with a 512 MiB guard.

Nine SQL workloads are sent unchanged to every engine. The ninth is a diagnostic
COUNT(DISTINCT) formulation that produces the same three checked scalar values
for this fixture as the million-group subquery. It is an additional operator
shape, not a replacement for the failing workload. Every result, including primes,
throughput and memory queries, is checked against the independent Python oracle.
Both source RLS and an actual PostgreSQL permission-denial log must pass first.

Each workload has one fresh-process observation, a prime, then seven warm samples.
Startup/readiness are recorded separately. Storage/OS/source caches are not flushed;
there is no storage-cold claim. Engine order rotates per workload. Throughput is
a finite batch of 30 requests at concurrency two. A separate native observer
measures RSS, PSS, process high-water marks and cgroup memory during two concurrent
queries. Failed executions retain their memory observations in raw data but are
not presented as successful-query memory comparisons.

The runner checks source uptime/process state before every engine phase and at completion.
Source failure invalidates the comparison. It also records each engine's state
before shutdown, including OOM status. The observer excludes address-space-free
zombies; a race that prevents complete PSS access is marked incomplete instead of
being presented as zero PSS or aborting unrelated latency measurements.

The host is shared. Existing QE processes can map the same executable/libraries,
affecting shared-page/PSS/cgroup accounting. No existing service is stopped to
improve a result. This and the different authentication/response formats prevent
interpreting the suite as isolated execution-library timing or footprint.

## Publish and leave the host tidy

Retrieve JSON results, then render locally or on the host:

```sh
python3 benchmarks/report_qe_host.py results/hetzner-qe.json results/hetzner-qe.md
uv run --with matplotlib==3.10.6 benchmarks/plot_qe_host.py results/hetzner-qe.json results/hetzner-qe.png
```

Retain rejected queries and the 512 MiB guard control. Never publish the API-token
file, auth databases or configuration secrets. Once the experiment is finished,
stop only the dedicated fixture containers; retain their files and the stopped
engine copies for reproduction:

```sh
docker stop neura-hz-20260928-postgres neura-hz-20260928-s3
```

Restart those two fixture containers before reusing the stopped engine profiles.
Never prune unrelated Docker resources. Older laptop Spark/DuckDB timings must not
be mixed numerically with this run's x86_64 results.
