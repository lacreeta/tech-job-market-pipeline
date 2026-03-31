import requests
import logging

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

    logger.info("Fetched %d jobs", len(jobs))
    
    return jobs