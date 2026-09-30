# Private Docker releases

Neura's Docker alpha releases are published to the **private** GHCR package
`ghcr.io/kritikmodi/neura`, linked to the private `kritikmodi/neura` repository.
Keep both private and restrict repository/package access to the owner. Build
automation has short-lived access; it does not make the images public.

## Install

Create a GitHub personal access token (classic) with `read:packages` for your own
account, then log in interactively. At the password prompt, enter that token.
Do not use your GitHub account password or put the token in configuration files.

```sh
docker login ghcr.io --username kritikmodi
docker pull ghcr.io/kritikmodi/neura:0.1.0-alpha.1
docker run --rm ghcr.io/kritikmodi/neura:0.1.0-alpha.1 --version
```

The multi-platform image selects native Linux AMD64 or ARM64 automatically.
The standard variant exposes REST. `0.1.0-alpha.1-cube` additionally includes the
optional PostgreSQL-wire listener. Both include the PostgreSQL source connector.

Prepare a private `neura.toml` and a mode-0600 environment file containing source
credential variables and `NEURA_API_TOKEN`. The configuration should include:

```toml
[limits]
memory_mb = 512
max_concurrent_queries = 2
spill_dir = "/var/lib/neura/spill"
max_spill_mb = 1024

[server]
http_addr = "0.0.0.0:8080"
allow_insecure_network = true
token_env = "NEURA_API_TOKEN"
```

Append the source definitions from `examples/sources.toml` as needed. Port 8080
is published only on host loopback. A remote deployment needs a private TLS proxy;
the image itself has no native listener TLS. Mounted `NAME_FILE` secrets and AWS
workload credentials are alternatives to literal values in an environment file.

```sh
export NEURA_CONFIG=/absolute/path/neura.toml
export NEURA_ENV_FILE=/absolute/path/protected-neura.env
docker compose -f deploy/compose.release.yml up -d
```

The release Compose file pulls images and never compiles local source. It enforces
2 CPUs and 2 GiB container memory with no additional swap. Spilling uses a Docker
disk volume; it is not a memory-backed tmpfs. The engine's spill cap is per query:
put the volume on a filesystem with an enforced aggregate quota for production.
Keep `spill_dir` pointed at that volume. Configuration and credentials stay outside
the image, and the process runs as UID/GID 10001 with a read-only root filesystem.

For Cube, additionally set `server.pg_addr="0.0.0.0:5433"` and provide
`NEURA_PG_PASSWORD` in the protected environment file. Leave `NEURA_IMAGE` unset
to use the Cube override's default image, or set it explicitly to a Cube digest.

```sh
docker compose -f deploy/compose.release.yml -f deploy/compose.release.cube.yml up -d
```

The optional PG port is also published only on loopback. For application
containers, use a shared private Docker network and the Neura service name.

## Upgrade and rollback

Use the exact version or immutable image digest from the private GitHub Release's
`release-manifest.json`. The same digest records both architectures. Set
`NEURA_IMAGE=ghcr.io/kritikmodi/neura@sha256:...` before running Compose to pin it.
Preserve the previous digest and configuration to roll back with the same command.
Drain in-flight requests before replacing a container: diagnostics are in memory,
and a release update restarts the process. Spill files are temporary, not source data.

## Publish the next alpha

1. Update the package version in `Cargo.toml` and its entry in `Cargo.lock`.
2. Push the reviewed commit to `main`.
3. Run the **Private Docker release** workflow on `main`:

   ```sh
   gh workflow run release.yml --repo kritikmodi/neura --ref main
   ```

The workflow refuses a public repository/package or an already tagged version.
It runs the existing unit/static and real PostgreSQL, ClickHouse, S3-compatible,
Iceberg and Cube integration checks. Native AMD64/ARM64 runners build both images
and test the actual registry digests for version/labels, non-root execution,
CLI/REST behavior, authorization, RLS, a PostgreSQL × S3 join and Cube compatibility.
No cloud credentials are required by this alpha pipeline; live AWS/Glue,
Databricks, Snowflake and BigQuery acceptance remains explicitly unverified.

Private `build-<commit>-<architecture>[-cube]` staging tags may exist while checks
are running or after a failed attempt. They are not released versions. Only after
all checks succeed does the workflow create multi-platform version tags and a
private GitHub prerelease. There is no moving `latest` tag. BuildKit attaches SBOM
and minimal build-provenance attestations without publishing to a public
transparency log. The release includes image digests and a checksummed manifest.

The publisher uses GitHub's short-lived workflow token with `packages:write` and
only grants `contents:write` to the final release job. No personal token is stored
as a repository secret. Privacy is checked before publication and after staging
and promotion; anonymous pulls must be denied. The publisher is deliberately
scoped to `kritikmodi/neura`. Changing access/visibility is an explicit owner action.

See [limitations](LIMITATIONS.md) before using an alpha against production data.
