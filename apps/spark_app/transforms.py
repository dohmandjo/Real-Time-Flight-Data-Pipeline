"""Pure PySpark transformations for the flight stream.

Nothing in this module touches Kafka, Postgres, or the filesystem, so every function can be
unit-tested against a small local DataFrame (see ``tests/test_spark_transforms.py``).

    raw JSON strings --parse_events--> typed, flattened columns --filter_events--> rows worth loading
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ---------------------------------------------------------------------
# Schema of the JSON the producer writes to Kafka (the message "value").
# Keep in sync with ``build_record`` in apps/producer/run_producer.py.
# ---------------------------------------------------------------------
SCHEMA = T.StructType([
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


# Spark's ISO-8601 timestamp pattern: optional .SSS fraction + numeric offset such as "+00:00".
TS_FMT = "yyyy-MM-dd'T'HH:mm:ss[.SSS]XXX"

# Aviationstack statuses we keep (compared case-insensitively):
#   "active" (in the air), "landed"/"arrived" (completed), "en-route"/"enroute" (in flight).
ACTIVE_STATUSES = ("active", "landed", "arrived", "en-route", "enroute")

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
    c = F.regexp_replace(c, r"([+-]\d{2})(\d{2})$", "$1:$2")      # '+0000' → '+00:00'

    # 2) Keep at most 3 fractional second digits
    c = F.regexp_replace(c, r"(\.\d{3})\d+", "$1")

    # 3A) If seconds are a single digit, pad to two (…:2 → …:02)
    c = F.regexp_replace(
        c,
        r"(T\d{2}:\d{2}:)(\d)(?=(?:\.\d{1,3}|[+-]\d{2}:\d{2}|$))",
        "$10$2"
    )

    # 3B) If seconds have three digits, drop the extra trailing one (keep two)
    c = F.regexp_replace(
        c,
        r"(T\d{2}:\d{2}:)(\d{2})\d(?=(?:\.\d{1,3}|[+-]\d{2}:\d{2}|$))",
        "$1$2"
    )

    # 3C) If seconds are missing entirely, add ':00'
    c = F.regexp_replace(
        c,
        r"(T\d{2}:\d{2})(?=(?:\.\d{1,3}|[+-]\d{2}:\d{2}|$))",
        "$1:00"
    )

    # 4) If we have no timezone at all (exactly yyyy-MM-ddTHH:mm:ss), assume UTC
    c = F.when(
        c.rlike(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$"),
        F.concat_ws("", c, F.lit("+00:00"))
    ).otherwise(c)

    return c

def parse_events(json_df: DataFrame) -> DataFrame:
    """Parse a DataFrame with a string column ``json`` into flat, typed columns.

    Unparseable timestamps become NULL instead of failing the stream.
    """
    return (
        json_df.select(F.from_json("json", SCHEMA).alias("r"))  # parse JSON to columns
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


def filter_events(df: DataFrame, days: int = 3, now: Column | None = None) -> DataFrame:
    """Keep rows that are worth loading into the warehouse.

    A row survives when ALL of these hold:
      * ``status`` (case-insensitive) is one of ``ACTIVE_STATUSES``;
      * it has a ``flight_key`` and at least one parsed timestamp;
      * at least one of dep/arr scheduled/actual falls within the last ``days`` days.

    ``now`` defaults to ``current_timestamp()``; tests pass a fixed value for determinism.
    """
    now = now if now is not None else F.current_timestamp()
    cutoff = now - F.expr(f"INTERVAL {int(days)} DAYS")
    timestamps = [F.col(c) for c in ("dep_sched_ts", "arr_sched_ts", "dep_act_ts", "arr_act_ts")]

    has_recent_ts = F.lit(False)
    has_any_ts = F.lit(False)
    for ts in timestamps:
        has_recent_ts = has_recent_ts | (ts.isNotNull() & (ts >= cutoff))
        has_any_ts = has_any_ts | ts.isNotNull()

    return (
        df.withColumn("status_lc", F.lower(F.col("status")))
          .filter(F.col("status_lc").isin(*ACTIVE_STATUSES))
          .filter(has_recent_ts)
          .filter(F.col("flight_key").isNotNull() & has_any_ts)
    )
