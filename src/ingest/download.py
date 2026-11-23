import argparse
import tempfile
from pathlib import Path

import requests
from google.cloud import storage

from src.common.config import load_config
from src.common.logger import get_logger

log = get_logger("ingest")
HEADERS = {"User-Agent": "Mozilla/5.0 (nyc-taxi-lakehouse)"}


def download_file(url: str, dest: Path) -> Path:
    log.info(f"Downloading {url}")
    with requests.get(url, headers=HEADERS, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                f.write(chunk)
    log.info(f"Saved {dest.name} ({dest.stat().st_size / 1e6:.1f} MB)")
    return dest


def upload_to_gcs(local: Path, bucket_name: str, blob_path: str) -> str:
    client = storage.Client()
    blob = client.bucket(bucket_name).blob(blob_path)
    blob.upload_from_filename(str(local))
    uri = f"gs://{bucket_name}/{blob_path}"
    log.info(f"Uploaded to {uri}")
    return uri


def run(year: int, month: int) -> dict:
    cfg = load_config(year, month)
    bucket = cfg["bucket"]
    trip_name = f"yellow_tripdata_{year}-{month:02d}.parquet"

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        trip_local = download_file(cfg["source"]["trip_url"], tmp / trip_name)
        zone_local = download_file(cfg["source"]["zone_url"], tmp / "taxi_zone_lookup.csv")

        trip_uri = upload_to_gcs(trip_local, bucket, f"raw/yellow/year={year}/month={month:02d}/{trip_name}")
        zone_uri = upload_to_gcs(zone_local, bucket, "raw/zones/taxi_zone_lookup.csv")

    return {"trip_uri": trip_uri, "zone_uri": zone_uri}


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Download NYC TLC data to GCS raw layer")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--month", type=int, required=True)
    args = p.parse_args()
    run(args.year, args.month)
