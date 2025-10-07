# cdc-kafka-iceberg-demo
This repo shows a production-style pattern for  Change Data Capture (CDC): Postgres  emits changes via Debezium (Kafka Connect) →  Kafka → PySpark Structured Streaming → Apache Iceberg table for analytics, time travel &amp; schema evolution.
# PostgreSQL → Debezium → Kafka → PySpark → Iceberg (CDC Lakehouse Demo)

This repo shows a production-style pattern for **Change Data Capture (CDC)**:
**Postgres** emits changes via **Debezium** (Kafka Connect) → **Kafka** →
**PySpark Structured Streaming** → **Apache Iceberg** table for analytics, time travel & schema evolution.

## Architecture

```mermaid
flowchart LR
  PG[(Postgres\nlogical decoding)] -->|pgoutput| DBZ[Debezium / Kafka Connect]
  DBZ -->|Debezium JSON| K[Kafka Topic\npg.public.customers]
  K --> S[PySpark Structured Streaming]
  S --> I[Iceberg Table\nlocal.demo.customers]
  I --> Q[Queries (spark-sql/Trino/DuckDB)]
mermaid

```mermaid
sequenceDiagram
  participant PG as Postgres
  participant DBZ as Debezium/Connect
  participant K as Kafka
  participant SP as Spark
  participant IC as Iceberg

  PG->>DBZ: logical replication (pgoutput)
  DBZ->>K: publish CDC messages
  loop every ~2s
    SP->>K: poll offsets
    K-->>SP: micro-batch of events
    SP->>IC: MERGE (upsert/delete)
  end
Repo structure
bash
mermaid

.
├─ docker-compose.yml            # zookeeper, kafka, connect (debezium), postgres
├─ init.sql                      # seeds public.customers
├─ register-debezium.json        # Debezium 2.x config (uses topic.prefix)
├─ cdc_to_iceberg.py             # Spark stream: Kafka → Iceberg MERGE
├─ spark-sql-iceberg.sh          # helper to query Iceberg
├─ .env.example                  # env vars for local runs
├─ .gitignore
└─ README.md
Prereqs
Docker Desktop (WSL2 integration ON if on Windows)

Java 17 and Spark 3.5.x on WSL/Linux (for spark-sql / local PySpark)

Python 3.8+ (optional venv)

Quickstart
1) Start the stack
bash
Copy code
docker compose up -d
curl -s http://localhost:8083/connectors
2) Register Debezium (Postgres connector)
bash
Copy code
curl -s -X POST -H "Content-Type: application/json" \
  --data-binary @register-debezium.json \
  http://localhost:8083/connectors
Status:

bash
Copy code
curl -s http://localhost:8083/connectors/pg-customers/status | python3 -m json.tool
# state should be RUNNING
3) Verify the Kafka topic
bash
Copy code
docker exec -it $(docker ps -qf name=kafka) \
  kafka-topics --bootstrap-server kafka:9092 --list

docker exec -it $(docker ps -qf name=kafka) \
  kafka-console-consumer --bootstrap-server kafka:9092 \
  --topic pg.public.customers --from-beginning --max-messages 5
4) Run the PySpark stream (from editor or CLI)
bash
Copy code
export KAFKA_BOOTSTRAP=localhost:9094
export KAFKA_TOPIC=pg.public.customers
export ICEBERG_WAREHOUSE=$HOME/iceberg-warehouse
export CHECKPOINT_DIR=$HOME/iceberg-checkpoints/customers
python cdc_to_iceberg.py
# logs: [epoch N] upserted X rows
Want a one-shot ETL? Change the trigger to .trigger(availableNow=True) to catch up and exit.

5) Poke CDC in Postgres
bash
Copy code
docker exec -it $(docker ps -qf name=postgres) psql -U demo -d demo -c \
"INSERT INTO public.customers(name,email) VALUES ('Grace Hopper','grace@example.com'); \
 UPDATE public.customers SET email='alan@turing.org' WHERE name='Alan Turing'; \
 DELETE FROM public.customers WHERE name='Ada Lovelace';"
6) Query the Iceberg table
If stream is stopped:

bash
Copy code
./spark-sql-iceberg.sh -e "SELECT id,name,email FROM local.demo.customers ORDER BY id"
If stream is running, avoid Derby lock:

bash
Copy code
./spark-sql-iceberg.sh \
  --conf "spark.hadoop.javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=$HOME/metastore_db_sql;create=true" \
  -e "SELECT id,name,email FROM local.demo.customers ORDER BY id"
Schema evolution demo
Upstream:

bash
Copy code
docker exec -it $(docker ps -qf name=postgres) psql -U demo -d demo -c \
"ALTER TABLE public.customers ADD COLUMN age INT; \
 INSERT INTO public.customers(name,email,age) VALUES ('Barbara Liskov','barbara@example.com',85);"
Iceberg:

bash
Copy code
./spark-sql-iceberg.sh -e "ALTER TABLE local.demo.customers ADD COLUMN age INT"
Update cdc_to_iceberg.py:

add StructField("age", IntegerType(), True) to after_schema

add col("r.after.age").alias("age") to the selected columns

update MERGE to set/insert age

Restart the stream and query again.

Time travel (Iceberg snapshots)
bash
Copy code
./spark-sql-iceberg.sh -e \
"SELECT snapshot_id, committed_at, operation FROM local.demo.customers.snapshots ORDER BY committed_at DESC"

# pick a snapshot_id from above:
./spark-sql-iceberg.sh -e \
"SELECT * FROM local.demo.customers VERSION AS OF <snapshot_id> ORDER BY id"
Troubleshooting
Connect 8083 refused: docker compose logs -f connect until “Herder started”.

Debezium 400: topic.prefix required: you’re on Debezium 2.x → use topic.prefix (this repo already does).

500 unexpected character: your JSON had // comments → remove comments.

UnknownTopicOrPartitionException: connector not running or topic not created; wait until step 3 shows the topic.

spark-sql hangs / Derby lock: stream already holds metastore_db lock; either stop stream or give spark-sql its own ConnectionURL (see above).

Empty micro-batch: no new CDC since last offsets; insert/update/delete in Postgres to trigger events.

Cleanup
bash
Copy code
docker compose down -v    # remove containers + volumes
rm -rf $HOME/iceberg-warehouse $HOME/iceberg-checkpoints