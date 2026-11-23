import argparse

from src.common.config import load_config
from src.common.logger import get_logger
from src.common.spark import get_spark
from src.gold.transforms import daily_revenue, payment_mix, zone_demand

log = get_logger("gold")

GOLD_TABLES = {
    "daily_revenue": daily_revenue,
    "zone_demand": zone_demand,
    "payment_mix": payment_mix,
}


def run(year: int, month: int) -> dict:
    cfg = load_config(year, month)
    spark = get_spark("gold", cfg)

    silver = (
        spark.read.format("delta").load(cfg["paths"]["silver"])
        .where(f"year = {year} AND month = {month}")
        .cache()
    )
    log.info(f"Read {silver.count():,} Silver rows for {year}-{month:02d}")

    counts = {}
    for name, fn in GOLD_TABLES.items():
        path = f"{cfg['paths']['gold']}/{name}"
        df = fn(silver)
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"year = {year} AND month = {month}")
            .partitionBy("year", "month")
            .save(path)
        )
        counts[name] = spark.read.format("delta").load(path).where(f"year = {year} AND month = {month}").count()
        log.info(f"{name}: {counts[name]:,} rows -> {path}")

    silver.unpersist()
    spark.stop()
    return counts


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Silver -> Gold (Delta)")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--month", type=int, required=True)
    args = p.parse_args()
    run(args.year, args.month)
