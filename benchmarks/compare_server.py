#!/usr/bin/env python3
"""Benchmark-only authenticated HTTP adapter. Real DuckDB/Spark source readers.

Only exact workload/probe statements are accepted; this is not a product API.
"""

import hmac
import json
import os
import pathlib
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ENGINE = os.environ["BENCH_ENGINE"]
TOKEN = os.environ["NEURA_API_TOKEN"]
MANIFEST = json.loads(pathlib.Path("/fixture/manifest.json").read_text())
WORKLOADS = json.loads(pathlib.Path("/bench/workloads/compare.json").read_text())
PROBES = ["SELECT 1", "SELECT * FROM pg.forbidden", "SELECT COUNT(*) FROM pg.customers WHERE tenant_id <> 1"]
ALLOWED = {w["sql"] for w in WORKLOADS} | set(PROBES)
SLOTS = threading.BoundedSemaphore(2)
paths = lambda kind, scheme: [f"{scheme}://neura-fixture/{f['path']}" for f in MANIFEST[kind]]

if ENGINE == "duckdb":
    import duckdb
    db = duckdb.connect(config={"threads": 2, "memory_limit": "512MiB",
        "temp_directory": "/tmp/spill", "max_temp_directory_size": "4GiB",
        "extension_directory": "/extensions", "enable_external_file_cache": False})
    db.execute("LOAD httpfs; LOAD postgres")
    db.execute("""CREATE SECRET fixture (TYPE S3, KEY_ID 'neura-reader',
        SECRET 'neura-test-readonly-storage', REGION 'us-east-1', ENDPOINT 's3:8333',
        URL_STYLE 'path', USE_SSL false)""")
    db.execute("""ATTACH 'host=postgres port=5432 dbname=neura user=neura_reader password=neura-test-reader-only'
        AS source_pg (TYPE postgres, READ_ONLY)""")
    db.execute("CREATE SCHEMA pg; CREATE SCHEMA lake; CREATE SCHEMA local")
    db.execute("CREATE VIEW pg.customers AS SELECT * FROM source_pg.public.customers")
    # The forbidden table is referenced directly in the negative permission probe.
    for table, kind in [("orders", "small"), ("orders_coarse", "large")]:
        db.execute(f"CREATE VIEW lake.{table} AS SELECT * FROM read_parquet({paths(kind, 's3')!r})")
    db.execute("CREATE VIEW local.orders AS SELECT * FROM read_parquet(['/fixture/large-0.parquet','/fixture/large-1.parquet'])")
    settings = dict(db.execute("SELECT name, value FROM duckdb_settings() WHERE name IN "
        "('threads','memory_limit','enable_external_file_cache','pg_use_ctid_scan','pg_experimental_filter_pushdown')").fetchall())
    metadata = {"version": duckdb.__version__, "settings": settings,
        "extensions": db.execute("SELECT extension_name, extension_version FROM duckdb_extensions() WHERE loaded").fetchall()}

    def execute(sql):
        with db.cursor() as connection:
            return connection.execute(sql.replace("pg.forbidden", "source_pg.public.forbidden")).fetchall()
else:
    # The fixture image has a passwd entry for nobody. Keep JVM caches on the
    # writable temporary mount, independent of the account's home directory.
    os.environ.setdefault("JAVA_TOOL_OPTIONS", "-Duser.home=/tmp")
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master("local[2]").appName("neura-spark-duckdb-comparison")
        .config("spark.sql.shuffle.partitions", "2").config("spark.default.parallelism", "2")
        .config("spark.sql.adaptive.enabled", "true").config("spark.ui.enabled", "false")
        .config("spark.scheduler.mode", "FAIR")
        .config("spark.hadoop.fs.s3a.endpoint", "http://s3:8333")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.access.key", "neura-reader")
        .config("spark.hadoop.fs.s3a.secret.key", "neura-test-readonly-storage")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.jars", "/jars/postgresql.jar,/jars/hadoop-aws.jar,/jars/aws-java-sdk-bundle.jar")
        .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    def jdbc(table):
        return (spark.read.format("jdbc").option("url", "jdbc:postgresql://postgres:5432/neura")
            .option("user", "neura_reader").option("password", "neura-test-reader-only")
            .option("dbtable", "public." + table).option("driver", "org.postgresql.Driver").load())

    jdbc("customers").createOrReplaceTempView("customers")
    for view, kind in [("orders", "small"), ("coarse_orders", "large")]:
        spark.read.parquet(*paths(kind, "s3a")).createOrReplaceTempView(view)
    spark.read.parquet("/fixture/large-0.parquet", "/fixture/large-1.parquet").createOrReplaceTempView("local_orders")
    metadata = {"version": spark.version, "settings": {"master": "local[2]", "driver_heap": "512m",
        "adaptive_execution": True, "shuffle_partitions": 2, "scheduler": "FAIR", "cached_tables": False}}

    def execute(sql):
        if sql == "SELECT * FROM pg.forbidden":
            return jdbc("forbidden").collect()
        for name, view in [("pg.customers", "customers"), ("lake.orders_coarse", "coarse_orders"),
                ("lake.orders", "orders"), ("local.orders", "local_orders")]:
            sql = sql.replace(name, view)
        return spark.sql(sql).collect()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def authorized(self):
        return hmac.compare_digest(self.headers.get("Authorization", ""), "Bearer " + TOKEN)

    def reply(self, status, body):
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        if not self.authorized():
            return self.reply(401, {"error": "unauthorized"})
        self.reply(200, {"status": "ready", **metadata})

    def do_POST(self):
        if not self.authorized():
            return self.reply(401, {"error": "unauthorized"})
        try:
            size = int(self.headers.get("Content-Length", "0"))
            if not 0 < size <= 16384:
                return self.reply(400, {"error": "invalid request size"})
            sql = json.loads(self.rfile.read(size))["sql"]
            if sql not in ALLOWED:
                return self.reply(400, {"error": "query is not a benchmark workload"})
            with SLOTS:
                result = execute(sql)
                rows = [[None if v is None else str(v) for v in row] for row in result]
            self.reply(200, {"rows": rows})
        except Exception as error:
            self.reply(500, {"error": str(error)})


ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
