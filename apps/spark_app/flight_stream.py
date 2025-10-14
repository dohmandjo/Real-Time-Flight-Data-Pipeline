import os
from pyspark.sql import SparkSession, functions as F, types as T

# ---------- JDBC / Kafka / checkpoints ----------
pg_url  = f"jdbc:postgresql://{os.getenv('PGHOST','flight_postgres')}:{os.getenv('PGPORT','5432')}/{os.getenv('PGDATABASE','flight_pipeline')}"
pg_user = os.getenv("PGUSER", "flight_user")
pg_pass = os.getenv("PGPASSWORD", "Password123")

CHECKPOINT = os.getenv("CHECKPOINT_DIR", "/tmp/chk/flights_stream")

# Default to your actual topic name used in Compose (flight_live)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP","kafka:9094")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC","flight_live")

# ---------- Spark ----------
spark = (
    SparkSession.builder
        .appName("flights_stream")
        .config("spark.sql.session.timeZone","UTC")
        .config("spark.jars.packages","org.postgresql:postgresql:42.7.3")
        .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("KAFKA_BOOTSTRAP =", KAFKA_BOOTSTRAP, flush=True)
print("KAFKA_TOPIC     =", KAFKA_TOPIC, flush=True)

raw = (
    spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets","latest")
        .option("failOnDataLoss","false")
        .load()
)

# ---------- Incoming JSON schema (matches producer) ----------
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

# Spark’s parser pattern
TS_FMT = "yyyy-MM-dd'T'HH:mm:ssXXX"

TS_FMT = "yyyy-MM-dd'T'HH:mm:ssXXX"

def clean_ts(colname: str):
    c = F.col(colname)

    # Normalize timezone forms first
    c = F.regexp_replace(c, r"Z$", "+00:00")                      # 'Z' -> '+00:00'
    c = F.regexp_replace(c, r"([+-]\d{2})(\d{2})$", r"\1:\2")     # '+0000' -> '+00:00'

    # Trim overly long millis (keep exactly 3)
    c = F.regexp_replace(c, r"(\.\d{3})\d+", r"\1")

    # ---- seconds normalization (robust) ----
    # A) Pad single-digit seconds (e.g. '...T12:34:2+00:00' -> '...T12:34:02+00:00')
    c = F.regexp_replace(
        c,
        r"(T\d{2}:\d{2}:)(\d)(?=(?:\.\d{1,3}|[+-]\d{2}:\d{2}|$))",
        r"\10\2"
    )

    # B) If we ever see three-digit seconds, drop the first digit (keep last two)
    c = F.regexp_replace(
        c,
        r"(T\d{2}:\d{2}:)\d(\d{2})(?=(?:\.\d{1,3}|[+-]\d{2}:\d{2}|$))",
        r"\1\2"
    )

    # C) If seconds are missing (only HH:mm present), append ':00'
    c = F.regexp_replace(
        c,
        r"(T\d{2}:\d{2})(?=(?:\.\d{1,3}|[+-]\d{2}:\d{2}|$))",
        r"\1:00"
    )

    # If we have exactly yyyy-MM-ddTHH:mm:ss with no TZ, add UTC offset
    c = F.when(
        c.rlike(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$"),
        F.concat_ws("", c, F.lit("+00:00"))
    ).otherwise(c)

    return c


parsed = (
    raw.select(F.col("value").cast("string").alias("json"))
       .select(F.from_json("json", schema).alias("r"))
       .select("r.*")
       # Parse timestamps safely (bad ones become NULL rather than crashing)
       .withColumn("dep_sched_ts", F.to_timestamp(clean_ts("departure.schedule"), TS_FMT))
       .withColumn("dep_est_ts",   F.to_timestamp(clean_ts("departure.estimated"), TS_FMT))
       .withColumn("dep_act_ts",   F.to_timestamp(clean_ts("departure.actual"), TS_FMT))
       .withColumn("arr_sched_ts", F.to_timestamp(clean_ts("arrival.schedule"), TS_FMT))
       .withColumn("arr_est_ts",   F.to_timestamp(clean_ts("arrival.estimated"), TS_FMT))
       .withColumn("arr_act_ts",   F.to_timestamp(clean_ts("arrival.actual"), TS_FMT))
       .withColumn("ingest_ts",    F.to_timestamp(clean_ts("ingest_time"), TS_FMT))
       .withColumn("dep_delay_min", F.col("departure.delay_min").cast("double"))
       .withColumn("arr_delay_min", F.col("arrival.delay_min").cast("double"))
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

# Keep ARRIVED / EN-ROUTE only, last 3 days
# Accept typical values from Aviationstack: active (flying), landed/arrived, en-route/enroute
statuses_keep = F.array(
    F.lit("active"), F.lit("landed"), F.lit("arrived"),
    F.lit("en-route"), F.lit("enroute")
)
three_days_ago = F.expr("current_timestamp() - INTERVAL 3 DAYS")

filtered = (
    parsed
      .withColumn("status_lc", F.lower(F.col("status")))
      .filter(F.array_contains(statuses_keep, F.col("status_lc")))
      .filter(
          (F.col("dep_sched_ts").isNotNull() & (F.col("dep_sched_ts") >= three_days_ago)) |
          (F.col("arr_sched_ts").isNotNull() & (F.col("arr_sched_ts") >= three_days_ago)) |
          (F.col("dep_act_ts").isNotNull()   & (F.col("dep_act_ts")   >= three_days_ago)) |
          (F.col("arr_act_ts").isNotNull()   & (F.col("arr_act_ts")   >= three_days_ago))
      )
      .filter(
          F.col("flight_key").isNotNull() &
          (
              F.col("dep_sched_ts").isNotNull() | F.col("arr_sched_ts").isNotNull() |
              F.col("dep_act_ts").isNotNull()   | F.col("arr_act_ts").isNotNull()
          )
      )
)

# ---------- Robust foreachBatch with diagnostics ----------
def write_batch(df, epoch_id: int):
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
        print(f"[foreachBatch] epoch={epoch_id} schema:", flush=True)
        out.printSchema()
        print(f"[foreachBatch] epoch={epoch_id} preview:", flush=True)
        for row in out.limit(5).collect():
            print("  ", row, flush=True)

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
        import traceback
        print(f"[foreachBatch] epoch={epoch_id} – JDBC write FAILED: {e}", flush=True)
        traceback.print_exc()
        # Re-raise so the micro-batch is visible in logs and we can fix quickly
        raise

# ---------- Start the stream ----------
query = (
    filtered.writeStream
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT)
        .foreachBatch(write_batch)
        .start()
)

print(">>> Streaming query started; checkpoint =", CHECKPOINT, flush=True)
query.awaitTermination()
