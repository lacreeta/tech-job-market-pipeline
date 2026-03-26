from typing import Any
from src.schemas.job_schema import CleanJob

def transform_jobs(raw_jobs: list[dict[str, Any]]) -> list[CleanJob]:
    """Transform raw job data into a structured format."""
    transformed_jobs: list[CleanJob] = []
    
    for job in raw_jobs:
        transformed_job: CleanJob = {
            "source": "remotive",
            "title": job.get("title"),
            "company": job.get("company_name"),
            "location": job.get("candidate_required_location"),
            "remote_type": "remote",
            "publication_date": job.get("publication_date"),
            "job_url": job.get("url"),
            "description": job.get("description"),
            "skills": job.get("tags", []),
            "category": job.get("category"),
        }

        if (
            transformed_job["title"] and
            transformed_job["company"] and
            transformed_job["job_url"]
        ):
            transformed_jobs.append(transformed_job)
    
    return transformed_jobs