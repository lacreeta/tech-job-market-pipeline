from airflow.sdk import dag, task
from pendulum import datetime
from src.extract.jobs_api import fetch_jobs
from src.transform.jobs_transformer import transform_jobs


@dag(
    start_date=datetime(2026,3,26, tz="Europe/Madrid"),
    schedule="@daily",
    catchup=False,
    tags=["jobs", "etl", "portfolio"]
)
def tech_jobs_pipeline():

    @task
    def extract_jobs():
        """Extract job data from the API."""
        jobs = fetch_jobs()
        print(f"Raw: {len(jobs)} jobs")
        return jobs

    @task
    def transform_jobs_task(raw_jobs):
        """Transform raw job data into a structured format."""
        clean_jobs = transform_jobs(raw_jobs)
        print(f"Clean: {len(clean_jobs)} jobs")
        return clean_jobs
    
    @task
    def save_processed_task(clean_jobs):
        """Save the processed job data to a JSON file."""
        from src.utils.file_utils import save_processed_jobs

        saved_path = save_processed_jobs(clean_jobs)
        print(f"Processed jobs saved to: {saved_path}")
        return saved_path

    @task
    def load_jobs_task(clean_jobs):
        """Load the processed job data into the database."""
        from src.load.postgres_loader import load_jobs

        print(f"Loading {len(clean_jobs)} jobs into the database")
        load_jobs(clean_jobs)

    # Setting up task dependencies
    raw_jobs = extract_jobs()
    transformed_jobs = transform_jobs_task(raw_jobs)
    saved = save_processed_task(transformed_jobs)
    loaded = load_jobs_task(transformed_jobs)
    
    saved >> loaded

# Instantiate the DAG
tech_jobs_pipeline()