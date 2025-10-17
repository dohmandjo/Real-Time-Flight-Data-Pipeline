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

 Author: you ✨
=========================================================================== 
"""

import os
from pyspark.sql import SparkSession, functions as F, types as T

# ---------------------------------------------------------------------
# JDBC / Kafka / checkpoint configuration
# ---------------------------------------------------------------------
# Build a JDBC connection string to Postgres using environment variables.
# Defaults match the docker-compose environment so it "just works" locally.
pg_url  = f"jdbc:postgresql://{os.getenv('PGHOST','flight_postgres')}:{os.getenv('PGPORT','5432')}/{os.getenv('PGDATABASE','flight_pipeline')}"
pg_user = os.getenv("PGUSER", "flight_user")
pg_pass = os.getenv("PGPASSWORD", "Password123")

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
# Define the schema of the JSON we expect in Kafka message "value".
# This mirrors the producer’s output so Spark can parse it efficiently.
# ---------------------------------------------------------------------
schema = T.StructType([
    T.StructField("flight_key",   T.StringType()),
    T.StructField("flight_date",  T.StringType()),
    T.StructField("status",       T.StringType()),
    T.StructField("airline", T.StructType([
        T.StructField("iata", T.StringType()),
        T.StructField("icao", T.StringType()),
        T.StructField("name", T.StringType())
    ])),
    T.StructField("flight", T.StructType([
        T.StructField("number", T.StringType()),
        T.StructField("iata",   T.StringType()),
        T.StructField("icao",   T.StringType())
    ])),
    T.StructField("departure", T.StructType([
        T.StructField("airport",   T.StringType()),
        T.StructField("iata",      T.StringType()),
        T.StructField("icao",      T.StringType()),
        T.StructField("gate",      T.StringType()),
        T.StructField("terminal",  T.StringType()),
        T.StructField("schedule",  T.StringType()),
        T.StructField("estimated", T.StringType()),
        T.StructField("actual",    T.StringType()),
        T.StructField("delay_min", T.IntegerType())
    ])),
    T.StructField("arrival", T.StructType([
        T.StructField("airport",   T.StringType()),
        T.StructField("iata",      T.StringType()),
        T.StructField("icao",      T.StringType()),
        T.StructField("gate",      T.StringType()),
        T.StructField("terminal",  T.StringType()),
        T.StructField("schedule",  T.StringType()),
        T.StructField("estimated", T.StringType()),
        T.StructField("actual",    T.StringType()),
        T.StructField("delay_min", T.IntegerType())
    ])),
    T.StructField("ingest_time", T.StringType()),
    T.StructField("source",      T.StringType())
])

# Spark’s ISO-8601 timestamp parse pattern with timezone like “+00:00”.
TS_FMT = "yyyy-MM-dd'T'HH:mm:ssXXX"

def clean_ts(colname: str):
    """
    Normalize timestamp strings into a format Spark can parse with TS_FMT.
    Handles:
      - 'Z' -> '+00:00'
      - '+0000' -> '+00:00'
      - trims milliseconds to 3 digits
      - pads single-digit seconds, clamps 3-digit seconds, adds ':00' if missing
      - appends '+00:00' if no timezone is present
    Returns a transformed column expression (no data movement yet).
    """
    c = F.col(colname)

    # 1) Normalize timezone symbols & shapes
    c = F.regexp_replace(c, r"Z$", "+00:00")                      # 'Z' to explicit UTC offset
    c = F.regexp_replace(c, r"([+-]\d{2})(\d{2})$", r"\1:\2")     # '+0000' → '+00:00'

    # 2) Keep at most 3 fractional second digits
    c = F.regexp_replace(c, r"(\.\d{3})\d+", r"\1")

    # 3A) If seconds are a single digit, pad to two (…:2 → …:02)
    c = F.regexp_replace(
        c,
        r"(T\d{2}:\d{2}:)(\d)(?=(?:\.\d{1,3}|[+-]\d{2}:\d{2}|$))",
        r"\10\2"
    )

    # 3B) If seconds have three digits, drop the extra trailing one (keep two)
    c = F.regexp_replace(
        c,
        r"(T\d{2}:\d{2}:)(\d{2})\d(?=(?:\.\d{1,3}|[+-]\d{2}:\d{2}|$))",
        r"\1\2"
    )

    # 3C) If seconds are missing entirely, add ':00'
    c = F.regexp_replace(
        c,
        r"(T\d{2}:\d{2})(?=(?:\.\d{1,3}|[+-]\d{2}:\d{2}|$))",
        r"\1:00"
    )

    # 4) If we have no timezone at all (exactly yyyy-MM-ddTHH:mm:ss), assume UTC
    c = F.when(
        c.rlike(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$"),
        F.concat_ws("", c, F.lit("+00:00"))
    ).otherwise(c)

    return c

# ---------------------------------------------------------------------
# Parse the Kafka JSON and derive normalized, analytics-ready columns
# ---------------------------------------------------------------------
parsed = (
    raw.select(F.col("value").cast("string").alias("json"))   # bytes → string
       .select(F.from_json("json", schema).alias("r"))        # parse JSON to columns
       .select("r.*")                                         # flatten one level
       # Parse all time fields safely; unparsable → NULL (no job crash)
       .withColumn("dep_sched_ts", F.to_timestamp(clean_ts("departure.schedule"), TS_FMT))
       .withColumn("dep_est_ts",   F.to_timestamp(clean_ts("departure.estimated"), TS_FMT))
       .withColumn("dep_act_ts",   F.to_timestamp(clean_ts("departure.actual"), TS_FMT))
       .withColumn("arr_sched_ts", F.to_timestamp(clean_ts("arrival.schedule"), TS_FMT))
       .withColumn("arr_est_ts",   F.to_timestamp(clean_ts("arrival.estimated"), TS_FMT))
       .withColumn("arr_act_ts",   F.to_timestamp(clean_ts("arrival.actual"), TS_FMT))
       .withColumn("ingest_ts",    F.to_timestamp(clean_ts("ingest_time"), TS_FMT))
       # Make delays numeric (double) so they aggregate correctly downstream
       .withColumn("dep_delay_min", F.col("departure.delay_min").cast("double"))
       .withColumn("arr_delay_min", F.col("arrival.delay_min").cast("double"))
       # Flatten nested structs to simple columns for SQL and JDBC sink
       .withColumn("airline_iata", F.col("airline.iata"))
       .withColumn("airline_icao", F.col("airline.icao"))
       .withColumn("airline_name", F.col("airline.name"))
       .withColumn("flight_number", F.col("flight.number"))
       .withColumn("flight_iata",   F.col("flight.iata"))
       .withColumn("flight_icao",   F.col("flight.icao"))
       .withColumn("dep_airport",   F.col("departure.airport"))
       .withColumn("dep_airport_iata", F.col("departure.iata"))
       .withColumn("dep_airport_icao", F.col("departure.icao"))
       .withColumn("dep_gate",      F.col("departure.gate"))
       .withColumn("dep_terminal",  F.col("departure.terminal"))
       .withColumn("arr_airport",   F.col("arrival.airport"))
       .withColumn("arr_airport_iata", F.col("arrival.iata"))
       .withColumn("arr_airport_icao", F.col("arrival.icao"))
       .withColumn("arr_terminal",  F.col("arrival.terminal"))
       .withColumn("arr_gate",      F.col("arrival.gate"))
)

# ---------------------------------------------------------------------
# Filter to flights that are arrived or en-route, in the last 3 days
# ---------------------------------------------------------------------
# Aviationstack typically uses:
#   - "active" (currently flying)
#   - "landed" / "arrived" (completed)
#   - "en-route" / "enroute" (in flight)
statuses_keep = F.array(
    F.lit("active"), F.lit("landed"), F.lit("arrived"),
    F.lit("en-route"), F.lit("enroute")
)
three_days_ago = F.expr("current_timestamp() - INTERVAL 3 DAYS")

filtered = (
    parsed
      # lower() so we can match allowed values case-insensitively
      .withColumn("status_lc", F.lower(F.col("status")))
      .filter(F.array_contains(statuses_keep, F.col("status_lc")))
      # keep only rows with a relevant timestamp in the last 3 days
      .filter(
          (F.col("dep_sched_ts").isNotNull() & (F.col("dep_sched_ts") >= three_days_ago)) |
          (F.col("arr_sched_ts").isNotNull() & (F.col("arr_sched_ts") >= three_days_ago)) |
          (F.col("dep_act_ts").isNotNull()   & (F.col("dep_act_ts")   >= three_days_ago)) |
          (F.col("arr_act_ts").isNotNull()   & (F.col("arr_act_ts")   >= three_days_ago))
      )
      # require a key and at least one populated time field (guards against junk)
      .filter(
          F.col("flight_key").isNotNull() &
          (
              F.col("dep_sched_ts").isNotNull() | F.col("arr_sched_ts").isNotNull() |
              F.col("dep_act_ts").isNotNull()   | F.col("arr_act_ts").isNotNull()
          )
      )
)

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
