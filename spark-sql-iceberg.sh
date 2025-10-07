#!/usr/bin/env bash
WAREHOUSE="${ICEBERG_WAREHOUSE:-$HOME/iceberg-warehouse}"
"$SPARK_HOME/bin/spark-sql" \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse="$WAREHOUSE" \
  "$@"
