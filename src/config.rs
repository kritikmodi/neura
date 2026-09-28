use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, path::Path};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub limits: Limits,
    #[serde(default)]
    pub server: Server,
    #[serde(default)]
    pub sources: Vec<Source>,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    pub memory_mb: usize,
    pub max_concurrent_queries: usize,
    pub max_connections: usize,
    pub query_timeout_secs: u64,
    pub batch_rows: usize,
    pub max_remote_page_mb: usize,
    pub max_output_mb: usize,
    pub partitions: usize,
    pub spill_dir: String,
    pub diagnostic_history: usize,
    pub max_spill_mb: u64,
    pub sort_merge_reservation_mb: usize,
    pub parquet_row_filter_pushdown: bool,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            memory_mb: 512,
            max_concurrent_queries: 4,
            max_connections: 64,
            query_timeout_secs: 120,
            batch_rows: 4096,
            max_remote_page_mb: 32,
            max_output_mb: 128,
            partitions: 4,
            spill_dir: "/tmp/neura-spill".into(),
            diagnostic_history: 100,
            max_spill_mb: 4096,
            sort_merge_reservation_mb: 16,
            parquet_row_filter_pushdown: false,
        }
    }
}
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Server {
    pub http_addr: String,
    /// Requires the optional postgres-wire build feature; disabled unless configured.
    pub pg_addr: Option<String>,
    pub token_env: String,
    pub pg_user: String,
    pub pg_password_env: String,
    pub allow_insecure_network: bool,
}
impl Default for Server {
    fn default() -> Self {
        Self {
            http_addr: "127.0.0.1:8080".into(),
            pg_addr: None,
            token_env: "NEURA_API_TOKEN".into(),
            pg_user: "neura".into(),
            pg_password_env: "NEURA_PG_PASSWORD".into(),
            allow_insecure_network: false,
        }
    }
}
#[derive(Clone, Deserialize)]
pub struct Source {
    pub name: String,
    #[serde(default)]
    pub tables: Vec<Table>,
    #[serde(flatten)]
    pub connection: Connection,
}
#[derive(Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Connection {
    Postgres {
        dsn_env: String,
        #[serde(default)]
        allow_insecure: bool,
    },
    Clickhouse {
        endpoint: String,
        user: String,
        password_env: String,
        #[serde(default)]
        allow_http: bool,
        /// Explicit acknowledgement of native statement-level consistency.
        consistency: String,
    },
    S3 {
        bucket: String,
        region: String,
        endpoint: Option<String>,
        #[serde(default)]
        allow_http: bool,
        #[serde(default = "iam_only")]
        governance: String,
    },
    Parquet {},
    Iceberg {},
    Glue {
        warehouse: String,
        region: String,
        #[serde(default = "iam_only")]
        governance: String,
    },
    Databricks {
        host: String,
        warehouse_id: String,
        token_env: String,
    },
    Snowflake {
        host: String,
        warehouse: String,
        database: String,
        schema: String,
        role: String,
        token_env: String,
    },
    Bigquery {
        project: String,
        location: String,
        token_env: String,
        maximum_bytes_billed: u64,
    },
}
fn iam_only() -> String {
    "unspecified".into()
}
impl Connection {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Postgres { .. } => "postgres",
            Self::Clickhouse { .. } => "clickhouse",
            Self::S3 { .. } => "s3",
            Self::Parquet { .. } => "parquet",
            Self::Iceberg { .. } => "iceberg",
            Self::Glue { .. } => "glue",
            Self::Databricks { .. } => "databricks",
            Self::Snowflake { .. } => "snowflake",
            Self::Bigquery { .. } => "bigquery",
        }
    }
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Table {
    pub name: String,
    pub remote: Option<String>,
    #[serde(default)]
    pub columns: Vec<Column>,
    #[serde(default)]
    pub files: Vec<FilePin>,
    pub metadata_location: Option<String>,
    pub snapshot_id: Option<i64>,
    /// Required for warehouse tables: Databricks version or UTC timestamp for Snowflake/BigQuery.
    pub snapshot: Option<String>,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilePin {
    pub path: String,
    pub etag: Option<String>,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Column {
    pub name: String,
    pub data_type: String,
    #[serde(default = "yes")]
    pub nullable: bool,
}
fn yes() -> bool {
    true
}

pub fn secret(name: &str) -> Result<String> {
    let value = if let Ok(path) = std::env::var(format!("{name}_FILE")) {
        std::fs::read_to_string(path)
            .with_context(|| format!("cannot read secret file for {name}"))?
            .trim_end_matches(['\r', '\n'])
            .to_string()
    } else {
        std::env::var(name)
            .with_context(|| format!("missing secret environment variable {name} or {name}_FILE"))?
    };
    ensure!(
        !value.is_empty(),
        "secret environment variable {name} is empty"
    );
    Ok(value)
}
pub fn identifier(s: &str) -> Result<()> {
    ensure!(
        !s.is_empty()
            && s.len() <= 128
            && s.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_')
            && !s.as_bytes()[0].is_ascii_digit(),
        "invalid identifier"
    );
    Ok(())
}
impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let c: Self = toml::from_str(&std::fs::read_to_string(path)?)?;
        c.validate()?;
        Ok(c)
    }
    pub fn validate(&self) -> Result<()> {
        let l = &self.limits;
        ensure!(
            l.memory_mb >= 32
                && l.sort_merge_reservation_mb > 0
                && l.sort_merge_reservation_mb < l.memory_mb
                && l.max_concurrent_queries > 0
                && l.max_connections > 0
                && l.partitions > 0
                && l.batch_rows > 0
                && l.batch_rows <= 65536
                && l.query_timeout_secs > 0
                && l.max_remote_page_mb > 0
                && l.max_output_mb > 0
                && l.diagnostic_history > 0,
            "invalid resource limits"
        );
        let mut sources = HashSet::new();
        let mut store_roots = HashSet::new();
        for s in &self.sources {
            identifier(&s.name)?;
            ensure!(
                !["public", "information_schema", "pg_catalog"].contains(&s.name.as_str()),
                "reserved source name"
            );
            match &s.connection {
                Connection::S3 { bucket, .. } => ensure!(
                    store_roots.insert(format!("s3://{bucket}")),
                    "one S3 source per bucket is required to isolate manifests and metrics"
                ),
                Connection::Parquet {} => ensure!(
                    store_roots.insert("file://".into()),
                    "use one local Parquet source with multiple tables"
                ),
                _ => {}
            }
            ensure!(sources.insert(&s.name), "duplicate source");
            match &s.connection {
                Connection::Clickhouse {
                    endpoint,
                    user,
                    password_env,
                    allow_http,
                    consistency,
                } => {
                    let u = url::Url::parse(endpoint)?;
                    ensure!(
                        (u.scheme() == "https" || (*allow_http && u.scheme() == "http"))
                            && u.host_str().is_some()
                            && u.username().is_empty()
                            && u.password().is_none()
                            && u.path() == "/"
                            && u.query().is_none()
                            && u.fragment().is_none(),
                        "ClickHouse endpoint must be an HTTPS origin without credentials; allow_http is for private test networks only"
                    );
                    ensure!(
                        !user.is_empty() && !password_env.is_empty(),
                        "ClickHouse user and password_env required"
                    );
                    ensure!(
                        consistency == "statement",
                        "ClickHouse requires consistency=statement; repeatable-read and time travel are unavailable"
                    );
                }
                Connection::S3 { governance, .. } | Connection::Glue { governance, .. } => ensure!(
                    governance == "iam_only",
                    "S3/Glue require explicit governance=iam_only; Lake Formation governed access is unsupported and must not use direct S3 reads"
                ),
                Connection::Databricks { host, .. } | Connection::Snowflake { host, .. } => {
                    let u = url::Url::parse(host)?;
                    ensure!(
                        u.scheme() == "https"
                            && u.host_str().is_some()
                            && u.username().is_empty()
                            && u.password().is_none()
                            && u.path() == "/"
                            && u.query().is_none(),
                        "warehouse host must be an HTTPS origin without credentials"
                    );
                }
                _ => {}
            }
            let mut names = HashSet::new();
            for t in &s.tables {
                identifier(&t.name)?;
                ensure!(names.insert(&t.name), "duplicate table");
                let mut cols = HashSet::new();
                for c in &t.columns {
                    ensure!(cols.insert(&c.name), "duplicate column");
                    crate::arrow_util::data_type(&c.data_type)?;
                }
                match &s.connection {
                    Connection::Postgres { .. }
                    | Connection::Clickhouse { .. }
                    | Connection::Databricks { .. }
                    | Connection::Snowflake { .. }
                    | Connection::Bigquery { .. } => {
                        let r = t.remote.as_deref().context("remote table required")?;
                        ensure!(
                            !t.columns.is_empty(),
                            "explicit remote table columns required"
                        );
                        ensure!(
                            (1..=3).contains(&r.split('.').count()),
                            "remote name requires 1-3 components"
                        );
                        for (i, part) in r.split('.').enumerate() {
                            if i == 0 && matches!(s.connection, Connection::Bigquery { .. }) {
                                identifier(&part.replace('-', "_"))?;
                            } else {
                                identifier(part)?;
                            }
                        }
                        if matches!(s.connection, Connection::Clickhouse { .. }) {
                            ensure!(
                                r.split('.').count() == 2,
                                "ClickHouse remote must be database.table"
                            );
                            ensure!(
                                t.snapshot.is_none() && t.snapshot_id.is_none(),
                                "ClickHouse snapshot pinning is unsupported"
                            );
                        }
                        if matches!(
                            s.connection,
                            Connection::Databricks { .. }
                                | Connection::Snowflake { .. }
                                | Connection::Bigquery { .. }
                        ) {
                            let snap=t.snapshot.as_deref().context("warehouse tables require snapshot (version for Databricks, RFC3339 timestamp otherwise)")?;
                            if matches!(s.connection, Connection::Databricks { .. }) {
                                snap.parse::<u64>()?;
                            } else {
                                chrono::DateTime::parse_from_rfc3339(snap)?;
                            }
                        }
                    }
                    Connection::Parquet {} | Connection::S3 { .. } => {
                        ensure!(
                            !t.files.is_empty(),
                            "Parquet requires an explicit immutable file manifest"
                        );
                        for f in &t.files {
                            if matches!(s.connection, Connection::S3 { .. }) {
                                ensure!(
                                    f.etag.as_deref().is_some_and(|e| !e.is_empty()),
                                    "each S3 object requires its ETag"
                                );
                            }
                        }
                    }
                    Connection::Iceberg {} => {
                        ensure!(
                            t.metadata_location.is_some(),
                            "Iceberg metadata_location required"
                        );
                    }
                    Connection::Glue { .. } => {
                        ensure!(
                            t.remote.as_ref().is_some_and(|v| v.split('.').count() == 2),
                            "Glue remote must be database.table"
                        );
                    }
                }
            }
        }
        ensure!(
            cfg!(feature = "postgres-wire") || self.server.pg_addr.is_none(),
            "pg_addr requires a build with --features postgres-wire; omit pg_addr for REST-only operation"
        );
        for addr in std::iter::once(&self.server.http_addr).chain(self.server.pg_addr.iter()) {
            let a: std::net::SocketAddr = addr.parse()?;
            if !a.ip().is_loopback() && !self.server.allow_insecure_network {
                bail!(
                    "non-loopback listener requires allow_insecure_network=true and an external TLS proxy/private network"
                );
            }
        }
        Ok(())
    }
}
