# cdc-kafka-iceberg-demo
This repo shows a production-style pattern for  Change Data Capture (CDC): Postgres  emits changes via Debezium (Kafka Connect) →  Kafka → PySpark Structured Streaming → Apache Iceberg table for analytics, time travel &amp; schema evolution.
