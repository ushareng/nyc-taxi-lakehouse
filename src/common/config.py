from pathlib import Path
import yaml

CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "config.yaml"


def load_config(year: int | None = None, month: int | None = None, path: Path = CONFIG_PATH) -> dict:
    cfg = yaml.safe_load(Path(path).read_text())
    if year:
        cfg["year"] = int(year)
    if month:
        cfg["month"] = int(month)
    cfg["paths"] = {k: v.format(bucket=cfg["bucket"]) for k, v in cfg["paths"].items()}
    cfg["source"]["trip_url"] = cfg["source"]["trip_url"].format(year=cfg["year"], month=cfg["month"])
    return cfg
