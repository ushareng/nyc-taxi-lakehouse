import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.config import load_config
from src.common.logger import get_logger
from src.common.spark import get_spark

log = get_logger("bronze")

BRONZE_SCHEMA = {
    "vendorid": "int",
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "passenger_count": "double",
    "trip_distance": "double",
    "ratecodeid": "double",
    "store_and_fwd_flag": "string",
    "pulocationid": "int",
    "dolocationid": "int",
    "payment_type": "int",
    "fare_amount": "double",
    "extra": "double",
    "mta_tax": "double",
    "tip_amount": "double",
    "tolls_amount": "double",
    "improvement_surcharge": "double",
    "total_amount": "double",
    "congestion_surcharge": "double",
    "airport_fee": "double",
}


def standardize_schema(df: DataFrame) -> DataFrame:
    """Lowercase column names and cast to a fixed schema; add missing columns as null."""
    df = df.toDF(*[c.lower() for c in df.columns])
    cols = []
    for name, dtype in BRONZE_SCHEMA.items():
        col = F.col(name) if name in df.columns else F.lit(None)
        cols.append(col.cast(dtype).alias(name))
    return df.select(*cols, *[c for c in df.columns if c == "source_file"])


def add_metadata(df: DataFrame, year: int, month: int) -> DataFrame:
    return (
        df.withColumn("ingest_date", F.current_date())
        .withColumn("year", F.lit(year).cast("int"))
        .withColumn("month", F.lit(month).cast("int"))
    )


def run(year: int, month: int) -> int:
    cfg = load_config(year, month)
    spark = get_spark("bronze", cfg)

    src = f"{cfg['paths']['raw']}/yellow/year={year}/month={month:02d}/"
    dest = cfg["paths"]["bronze"]
    log.info(f"Reading {src}")

    raw = spark.read.parquet(src).withColumn("source_file", F.input_file_name())
    df = add_metadata(standardize_schema(raw), year, month)

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"year = {year} AND month = {month}")
        .partitionBy("year", "month")
        .save(dest)
    )

    count = spark.read.format("delta").load(dest).where(f"year = {year} AND month = {month}").count()
    log.info(f"Wrote {count:,} rows to {dest} (year={year}, month={month})")
    spark.stop()
    return count


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Raw -> Bronze (Delta)")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--month", type=int, required=True)
    args = p.parse_args()
    run(args.year, args.month)
