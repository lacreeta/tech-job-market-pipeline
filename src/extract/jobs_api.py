import requests
import logging
from datetime import datetime
from pathlib import Path
from src.utils.file_utils import save_raw_jobs

logger = logging.getLogger(__name__)

def fetch_jobs(source="remotive") -> list[dict]:
    """Fetch job data from the API."""

    if source == "remotive":
        url = "https://remotive.com/api/remote-jobs"
    else:
        raise ValueError(f"Unknown source: {source}")

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    data = response.json()
    jobs = data.get("jobs", [])

    # guardar raw
    saved_path = save_raw_jobs(jobs)
    logger.info("Raw jobs saved to: %s", saved_path)
    
    return jobs