import argparse

from pyspark.sql import functions as F

from src.common.config import load_config
from src.common.logger import get_logger
from src.common.spark import get_spark
from src.silver.transforms import (
    add_derived_columns,
    flag_duplicates,
    flag_quality_issues,
    join_zones,
    prepare_zones,
    rename_and_cast,
    split_valid_quarantine,
)

log = get_logger("silver")


def write_partition(df, path: str, year: int, month: int) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"year = {year} AND month = {month}")
        .partitionBy("year", "month")
        .save(path)
    )


def run(year: int, month: int) -> dict:
    cfg = load_config(year, month)
    paths = cfg["paths"]
    spark = get_spark("silver", cfg)

    # Zones: raw CSV -> Delta (small, full overwrite)
    zones_raw = spark.read.option("header", True).csv(f"{paths['raw']}/zones/taxi_zone_lookup.csv")
    zones = prepare_zones(zones_raw)
    zones.write.format("delta").mode("overwrite").save(paths["zones"])
    log.info(f"Wrote {zones.count()} zones to {paths['zones']}")

    # Trips: Bronze -> Silver + Quarantine
    bronze = spark.read.format("delta").load(paths["bronze"]).where(f"year = {year} AND month = {month}")
    bronze_count = bronze.count()
    log.info(f"Read {bronze_count:,} Bronze rows for {year}-{month:02d}")

    df = rename_and_cast(bronze)
    df = add_derived_columns(df)
    df = flag_quality_issues(df, year, month)
    df = flag_duplicates(df).cache()

    valid, quarantine = split_valid_quarantine(df)
    valid = join_zones(valid, zones)

    write_partition(valid, paths["silver"], year, month)
    write_partition(quarantine, paths["quarantine"], year, month)

    silver_count = spark.read.format("delta").load(paths["silver"]).where(f"year = {year} AND month = {month}").count()
    q_count = spark.read.format("delta").load(paths["quarantine"]).where(f"year = {year} AND month = {month}").count()

    log.info(f"Silver: {silver_count:,} rows -> {paths['silver']}")
    log.info(f"Quarantine: {q_count:,} rows -> {paths['quarantine']}")
    log.info(f"Valid rate: {silver_count / bronze_count:.2%}")

    if silver_count + q_count != bronze_count:
        raise ValueError(f"Row mismatch: bronze={bronze_count}, silver={silver_count}, quarantine={q_count}")

    df.unpersist()
    spark.stop()
    return {"bronze": bronze_count, "silver": silver_count, "quarantine": q_count}


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Bronze -> Silver + Quarantine (Delta)")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--month", type=int, required=True)
    args = p.parse_args()
    run(args.year, args.month)
