import json
from datetime import datetime
from pathlib import Path
from typing import Any

BASE_DATA_DIR = Path("/opt/airflow/data")
RAW_DIR = BASE_DATA_DIR / "raw"
PROCESSED_DIR = BASE_DATA_DIR / "processed"


def _generate_timestamp() -> str:
    """Generates a timestamp string in the format YYYYMMDD_HHMMSS."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def save_raw_jobs(jobs: list[dict[str, Any]]) -> str:
    """Saves the raw job data to a JSON file in the specified output directory.

    Args:
        jobs: A list of dictionaries containing job data.

    Returns:
        The path to the saved JSON file.
    """
    path = RAW_DIR / f"jobs_raw_{_generate_timestamp()}.json"
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path,"w", encoding="utf-8") as f:
        json.dump(jobs, f, indent=2, ensure_ascii=False)

    return str(path)

def save_processed_jobs(jobs: list[dict[str, Any]]) -> str:
    """Saves the processed job data to a JSON file in the specified output directory.

    Args:
        jobs: A list of dictionaries containing processed job data.
    Returns:
        The path to the saved JSON file.
    """
    path = PROCESSED_DIR / f"jobs_processed_{_generate_timestamp()}.json"

    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path,"w", encoding="utf-8") as f:
        json.dump(jobs, f, indent=2, ensure_ascii=False)

    return str(path)