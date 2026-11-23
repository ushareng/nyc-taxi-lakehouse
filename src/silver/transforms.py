from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

MAX_TRIP_MINUTES = 24 * 60

RENAME_MAP = {
    "vendorid": ("vendor_id", "int"),
    "tpep_pickup_datetime": ("pickup_ts", "timestamp"),
    "tpep_dropoff_datetime": ("dropoff_ts", "timestamp"),
    "passenger_count": ("passenger_count", "int"),
    "trip_distance": ("trip_distance", "double"),
    "ratecodeid": ("rate_code_id", "int"),
    "store_and_fwd_flag": ("store_and_fwd_flag", "string"),
    "pulocationid": ("pu_location_id", "int"),
    "dolocationid": ("do_location_id", "int"),
    "payment_type": ("payment_type", "int"),
    "fare_amount": ("fare_amount", "double"),
    "extra": ("extra", "double"),
    "mta_tax": ("mta_tax", "double"),
    "tip_amount": ("tip_amount", "double"),
    "tolls_amount": ("tolls_amount", "double"),
    "improvement_surcharge": ("improvement_surcharge", "double"),
    "total_amount": ("total_amount", "double"),
    "congestion_surcharge": ("congestion_surcharge", "double"),
    "airport_fee": ("airport_fee", "double"),
}
KEEP_COLS = ["source_file", "ingest_date", "year", "month"]

DEDUP_KEYS = ["vendor_id", "pickup_ts", "dropoff_ts", "pu_location_id", "do_location_id", "fare_amount", "total_amount"]

PAYMENT_TYPES = {0: "Flex Fare", 1: "Credit card", 2: "Cash", 3: "No charge", 4: "Dispute", 5: "Unknown", 6: "Voided trip"}


def rename_and_cast(df: DataFrame) -> DataFrame:
    cols = [F.col(src).cast(t).alias(dst) for src, (dst, t) in RENAME_MAP.items()]
    return df.select(*cols, *[c for c in KEEP_COLS if c in df.columns])


def add_derived_columns(df: DataFrame) -> DataFrame:
    duration = (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60.0
    payment_map = F.create_map(*[x for k, v in PAYMENT_TYPES.items() for x in (F.lit(k), F.lit(v))])
    return (
        df.withColumn("trip_duration_min", F.round(duration, 2))
        .withColumn("pickup_date", F.to_date("pickup_ts"))
        .withColumn("pickup_hour", F.hour("pickup_ts"))
        .withColumn("payment_type_desc", F.coalesce(payment_map[F.col("payment_type")], F.lit("Unknown")))
    )


def flag_quality_issues(df: DataFrame, year: int, month: int) -> DataFrame:
    """Add a dq_reason column: empty string = valid, otherwise a comma-separated list of failed rules."""
    required = ["pickup_ts", "dropoff_ts", "pu_location_id", "do_location_id", "fare_amount"]
    null_check = F.greatest(*[F.col(c).isNull().cast("int") for c in required]) == 1

    rules = [
        F.when(null_check, F.lit("null_required")),
        F.when(F.col("fare_amount") <= 0, F.lit("non_positive_fare")),
        F.when(F.col("trip_duration_min") <= 0, F.lit("non_positive_duration")),
        F.when(F.col("trip_duration_min") > MAX_TRIP_MINUTES, F.lit("trip_over_24h")),
        F.when(
            (F.year("pickup_ts") != year) | (F.month("pickup_ts") != month),
            F.lit("outside_month"),
        ),
    ]
    return df.withColumn("dq_reason", F.concat_ws(",", *rules))


def flag_duplicates(df: DataFrame) -> DataFrame:
    """Mark every copy after the first of an identical trip as a duplicate."""
    w = Window.partitionBy(*DEDUP_KEYS).orderBy(F.col("source_file"))
    df = df.withColumn("_rn", F.row_number().over(w))
    reason = F.when(
        F.col("_rn") > 1,
        F.when(F.col("dq_reason") == "", F.lit("duplicate")).otherwise(F.concat_ws(",", "dq_reason", F.lit("duplicate"))),
    ).otherwise(F.col("dq_reason"))
    return df.withColumn("dq_reason", reason).drop("_rn")


def split_valid_quarantine(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid = df.where(F.col("dq_reason") == "").drop("dq_reason")
    quarantine = df.where(F.col("dq_reason") != "").withColumn("quarantined_at", F.current_timestamp())
    return valid, quarantine


def prepare_zones(zones_raw: DataFrame) -> DataFrame:
    return zones_raw.select(
        F.col("LocationID").cast("int").alias("location_id"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zone"),
        F.col("service_zone"),
    )


def join_zones(df: DataFrame, zones: DataFrame) -> DataFrame:
    pu = F.broadcast(zones.select(
        F.col("location_id").alias("pu_location_id"),
        F.col("borough").alias("pickup_borough"),
        F.col("zone").alias("pickup_zone"),
    ))
    do = F.broadcast(zones.select(
        F.col("location_id").alias("do_location_id"),
        F.col("borough").alias("dropoff_borough"),
        F.col("zone").alias("dropoff_zone"),
    ))
    return df.join(pu, "pu_location_id", "left").join(do, "do_location_id", "left")
