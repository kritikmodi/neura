"""Persistent Spark driver. Uses the same read-only source account and Parquet data."""

import json, os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("local[2]")
    .appName("neura-comparison")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.default.parallelism", "2")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.ui.enabled", "false")
    .config("spark.hadoop.fs.s3a.endpoint", "http://s3:8333")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.access.key", "neura-reader")
    .config("spark.hadoop.fs.s3a.secret.key", "neura-test-readonly-storage")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    .config(
        "spark.jars",
        "/jars/postgresql.jar,/jars/hadoop-aws.jar,/jars/aws-java-sdk-bundle.jar",
    )
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
# Spark temporary views cannot have multipart names; the SQL harness substitutes
# only the two catalog identifiers. All relational operations remain identical.
(
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://postgres:5432/neura")
    .option("user", "neura_reader")
    .option("password", "neura-test-reader-only")
    .option("dbtable", "public.customers")
    .option("driver", "org.postgresql.Driver")
    .load()
    .createOrReplaceTempView("customers")
)
spark.read.parquet("s3a://neura-fixture/orders/").createOrReplaceTempView("orders")


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'{"status":"ready"}')

    def do_POST(self):
        try:
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            sql = (
                body["sql"]
                .replace("pg.customers", "customers")
                .replace("lake.orders", "orders")
            )
            rows = spark.sql(sql).collect()
            result = {
                "rows": [[None if v is None else str(v) for v in r] for r in rows]
            }
            self.send_response(200)
        except Exception as e:
            result = {"error": str(e)}
            self.send_response(500)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(result).encode())


ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
