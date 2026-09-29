"""
===========================================================================
 Project: Real-Time Flight Data Pipeline — Spark Streaming App
 File:    spark_app/flight_stream.py

 PURPOSE
   Consume flight events from Kafka, normalize & filter them with PySpark
   Structured Streaming, then append cleaned rows into a Postgres staging
   table. A separate "loader" job upserts from staging into a star schema.

 WHAT THIS DOES (high level)
   1) Reads newline-delimited JSON messages from Kafka topic `flight_live`.
   2) Parses/validates fields using an explicit schema for reliability.
   3) Normalizes timestamps (handles Z / +0000, missing seconds, long millis).
   4) Keeps only flights that are arrived/en-route in the last 3 days.
   5) Writes each micro-batch to Postgres table: fact_flight_status_staging.

 DATA FLOW
   Producer (Aviationstack API) -> Kafka(topic=flight_live)
   -> Spark Structured Streaming (this file) -> Postgres (staging)
   -> Loader service -> Star schema (dim_*, fact_flight_status)
   -> View(s) -> Google Sheets sink -> Tableau

 INPUTS (ENV)
   KAFKA_BOOTSTRAP  : Kafka bootstrap servers (default: kafka:9094)
   KAFKA_TOPIC      : Kafka topic (default: flight_live)
   PGHOST/PGPORT/...: Postgres connection params
   CHECKPOINT_DIR   : Spark checkpoint path (default: /tmp/chk/flights_stream)

 OUTPUTS
   JDBC append into table: flight_pipeline.public.fact_flight_status_staging

 OPS NOTES
   - Checkpointing enables exactly-once semantics at the sink table level
     (combined with idempotent upsert in loader).
   - If the app restarts, it will resume from the last committed offsets.
   - Logging shows schema and preview of each micro-batch to aid debugging.

 TROUBLESHOOTING QUICK HINTS
   - If you see timestamp parse errors, verify the producer JSON and the
     clean_ts() regex steps; unparsable values become NULL (no crash).
   - If Postgres errors, confirm credentials, network (container names), and
     that the staging table exists (created by 00_warehouse.sql).
   - For empty batches, the app will skip JDBC writes (expected when no new
     data has arrived).

=========================================================================== 
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from transforms import filter_events, parse_events

# ---------------------------------------------------------------------
# JDBC / Kafka / checkpoint configuration
# ---------------------------------------------------------------------
# Build a JDBC connection string to Postgres using environment variables.
# Host/port/db defaults match docker-compose; the password has no default on purpose.
pg_url  = f"jdbc:postgresql://{os.getenv('PGHOST','flight_postgres')}:{os.getenv('PGPORT','5432')}/{os.getenv('PGDATABASE','flight_pipeline')}"
pg_user = os.getenv("PGUSER", "flight_user")
pg_pass = os.getenv("PGPASSWORD")
if not pg_pass:
    raise SystemExit("PGPASSWORD is not set (copy .env.example to .env).")

# Where Spark will keep state & progress info for the streaming job.
# This lets Spark recover from restarts without duplicating data.
CHECKPOINT = os.getenv("CHECKPOINT_DIR", "/tmp/chk/flights_stream")

# Kafka connection details: broker(s) + topic name that the producer writes to.
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP","kafka:9094")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC","flight_live")

# ---------------------------------------------------------------------
# Create the SparkSession
# ---------------------------------------------------------------------
# - Set SQL timezone to UTC so timestamps are consistent end-to-end.
# - Ensure the Postgres JDBC driver is available (also provided via --packages at runtime).
spark = (
    SparkSession.builder
        .appName("flights_stream")
        .config("spark.sql.session.timeZone","UTC")
        .config("spark.jars.packages","org.postgresql:postgresql:42.7.3")
        .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Log the effective Kafka config so we can verify the container is wired correctly.
print("KAFKA_BOOTSTRAP =", KAFKA_BOOTSTRAP, flush=True)
print("KAFKA_TOPIC     =", KAFKA_TOPIC, flush=True)

# ---------------------------------------------------------------------
# Read from Kafka as a streaming source
# ---------------------------------------------------------------------
# Kafka messages are key/value bytes. We only use the value, which contains JSON.
raw = (
    spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)  # broker address
        .option("subscribe", KAFKA_TOPIC)                    # topic to consume
        .option("startingOffsets","latest")                  # start at the end
        .option("failOnDataLoss","false")                    # tolerate retention gaps
        .load()
)

# ---------------------------------------------------------------------
# Parse and filter using the pure functions in transforms.py
# ---------------------------------------------------------------------
parsed = parse_events(raw.select(F.col("value").cast("string").alias("json")))  # bytes -> string -> columns
filtered = filter_events(parsed, days=3)

# ---------------------------------------------------------------------
# Per-microbatch sink to Postgres (staging table)
# ---------------------------------------------------------------------
def write_batch(df, epoch_id: int):
    """
    Called for every micro-batch:
      1) Skip empty batches (avoids JDBC edge cases).
      2) Print a small preview to logs (helps debugging in container).
      3) Append rows to fact_flight_status_staging via JDBC.
    """
    if df.rdd.isEmpty():
        print(f"[foreachBatch] epoch={epoch_id} – empty batch; skip", flush=True)
        return

    out = df.select(
        "flight_key",
        F.to_date("flight_date").alias("flight_date"),
        "status",
        F.coalesce(F.col("ingest_ts"), F.current_timestamp()).alias("ingest_time"),
        "flight_number","flight_iata","flight_icao",
        "airline_iata","airline_icao","airline_name",
        "dep_airport","dep_airport_iata","dep_airport_icao","dep_terminal","dep_gate",
        F.col("dep_sched_ts").alias("dep_scheduled"),
        F.col("dep_est_ts").alias("dep_estimated"),
        F.col("dep_act_ts").alias("dep_actual"),
        "dep_delay_min",
        "arr_airport","arr_airport_iata","arr_airport_icao","arr_terminal","arr_gate",
        F.col("arr_sched_ts").alias("arr_scheduled"),
        F.col("arr_est_ts").alias("arr_estimated"),
        F.col("arr_act_ts").alias("arr_actual"),
        "arr_delay_min"
    )

    try:
        # Lightweight observability
        print(f"[foreachBatch] epoch={epoch_id} schema:", flush=True)
        out.printSchema()
        print(f"[foreachBatch] epoch={epoch_id} preview:", flush=True)
        for row in out.limit(5).collect():
            print("  ", row, flush=True)

        # Append to the staging table; the loader service will upsert into the star schema.
        (out.write
            .format("jdbc")
            .option("url", pg_url)
            .option("dbtable", "fact_flight_status_staging")
            .option("user", pg_user)
            .option("password", pg_pass)
            .option("driver", "org.postgresql.Driver")
            .option("batchsize", "5000")
            .option("isolationLevel", "READ_COMMITTED")
            .mode("append")
            .save())
        print(f"[foreachBatch] epoch={epoch_id} – JDBC write OK", flush=True)

    except Exception as e:
        # Keep errors visible in logs for fast root cause analysis.
        import traceback
        print(f"[foreachBatch] epoch={epoch_id} – JDBC write FAILED: {e}", flush=True)
        traceback.print_exc()
        raise

# ---------------------------------------------------------------------
# Start the Structured Streaming query
# ---------------------------------------------------------------------
query = (
    filtered.writeStream
        .outputMode("append")                     # append-only stream
        .option("checkpointLocation", CHECKPOINT) # enable recovery
        .foreachBatch(write_batch)                # custom sink
        .start()
)

print(">>> Streaming query started; checkpoint =", CHECKPOINT, flush=True)
query.awaitTermination()
