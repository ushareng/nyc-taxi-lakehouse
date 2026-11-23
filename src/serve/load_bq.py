import argparse

from google.cloud import bigquery
from google.api_core.exceptions import NotFound

from src.common.config import load_config
from src.common.logger import get_logger
from src.common.spark import get_spark

log = get_logger("load_bq")

GOLD_TABLES = ["daily_revenue", "zone_demand", "payment_mix"]


def export_to_parquet(spark, cfg: dict, table: str, year: int, month: int) -> str:
    src = f"{cfg['paths']['gold']}/{table}"
    dest = f"gs://{cfg['bucket']}/gold_export/{table}/year={year}/month={month:02d}"
    df = spark.read.format("delta").load(src).where(f"year = {year} AND month = {month}")
    df.coalesce(1).write.mode("overwrite").parquet(dest)
    log.info(f"Exported {table} -> {dest}")
    return f"{dest}/*.parquet"


def delete_month(client: bigquery.Client, table_id: str, year: int, month: int) -> None:
    try:
        client.get_table(table_id)
    except NotFound:
        log.info(f"{table_id} does not exist yet; will be created")
        return
    q = f"DELETE FROM `{table_id}` WHERE year = {year} AND month = {month}"
    client.query(q).result()
    log.info(f"Deleted existing rows for {year}-{month:02d} from {table_id}")


def load_parquet(client: bigquery.Client, uri: str, table_id: str) -> None:
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    client.load_table_from_uri(uri, table_id, job_config=job_config).result()


def count_month(client: bigquery.Client, table_id: str, year: int, month: int) -> int:
    q = f"SELECT COUNT(*) AS n FROM `{table_id}` WHERE year = {year} AND month = {month}"
    return list(client.query(q).result())[0].n


def run(year: int, month: int) -> dict:
    cfg = load_config(year, month)
    spark = get_spark("load_bq", cfg)
    client = bigquery.Client(project=cfg["project_id"], location=cfg["region"])
    dataset = f"{cfg['project_id']}.{cfg['bigquery']['dataset']}"

    counts = {}
    for table in GOLD_TABLES:
        uri = export_to_parquet(spark, cfg, table, year, month)
        table_id = f"{dataset}.{table}"
        delete_month(client, table_id, year, month)
        load_parquet(client, uri, table_id)
        counts[table] = count_month(client, table_id, year, month)
        log.info(f"Loaded {counts[table]:,} rows into {table_id} for {year}-{month:02d}")

    spark.stop()
    return counts


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Gold (Delta) -> BigQuery")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--month", type=int, required=True)
    args = p.parse_args()
    run(args.year, args.month)
