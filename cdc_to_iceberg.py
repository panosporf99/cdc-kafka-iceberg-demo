#!/usr/bin/env python3
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType

# ---- Config (env overrides are supported)
WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", os.path.expanduser("~/iceberg-warehouse"))
CHECKPOINT = os.getenv("CHECKPOINT_DIR", os.path.expanduser("~/iceberg-checkpoints/customers"))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")  # our compose maps broker to 9094
TOPIC = os.getenv("KAFKA_TOPIC", "pg.public.customers")

# ---- Build SparkSession with required packages (works from "python cdc_to_iceberg.py")
spark = (
    SparkSession.builder
    .appName("CDC->Iceberg Customers")
    # Iceberg integration
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", WAREHOUSE)
    # Pull jars automatically (so you don't need --packages)
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"
        ])
    )
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---- Debezium envelope (minimal fields)
after_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True)
    # Add: StructField("age", IntegerType(), True) later for schema evolution
])
envelope_schema = StructType([
    StructField("op", StringType(), True),   # c,u,d,r
    StructField("ts_ms", LongType(), True),
    StructField("before", after_schema, True),
    StructField("after",  after_schema, True)
])

# ---- Create Iceberg table if needed
spark.sql("""
CREATE TABLE IF NOT EXISTS local.demo.customers (
  id INT,
  name STRING,
  email STRING
) USING iceberg
PARTITIONED BY (bucket(16, id))
""")

# ---- Read CDC from Kafka
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)
parsed = raw.selectExpr("CAST(value AS STRING) AS json")
records = parsed.select(from_json(col("json"), envelope_schema).alias("r"))

events = records.select(
    expr("coalesce(r.after.id, r.before.id)").alias("id"),
    col("r.after.name").alias("name"),
    col("r.after.email").alias("email"),
    col("r.op").alias("op")
).where(col("id").isNotNull())

def upsert(batch_df, epoch_id):
    # 1) skip empty batches
    if batch_df.rdd.isEmpty():
        print(f"[epoch {epoch_id}] empty micro-batch; skipping")
        return

    # 2) use the same session as this batch
    sess = batch_df.sparkSession

    # 3) prepare the view with the right types/columns
    prepared = batch_df.selectExpr(
        "CAST(id AS INT) AS id",
        "name",
        "email",
        "op"
    )
    prepared.createOrReplaceTempView("batch")

    # 4) MERGE using the view directly
    sess.sql("""
        MERGE INTO local.demo.customers t
        USING batch s
        ON t.id = s.id
        WHEN MATCHED AND s.op = 'd' THEN DELETE
        WHEN MATCHED AND s.op IN ('u','c','r') THEN UPDATE SET
            t.name = s.name,
            t.email = s.email
        WHEN NOT MATCHED AND s.op IN ('u','c','r') THEN INSERT (id, name, email)
            VALUES (s.id, s.name, s.email)
    """)

    print(f"[epoch {epoch_id}] upserted {prepared.count()} rows", flush=True)

q = (
    events.writeStream
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="2 seconds")
    .foreachBatch(upsert)
    .start()
)
q.awaitTermination()
