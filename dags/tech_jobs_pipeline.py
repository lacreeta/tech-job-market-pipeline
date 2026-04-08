from airflow.sdk import dag, task
from pendulum import datetime

def build_partition_from_context() -> str:
    from airflow.sdk import get_current_context
    return get_current_context()["data_interval_start"].format("DD-MM-YYYY")

@dag(
    start_date=datetime(2026,3,26, tz="Europe/Madrid"),
    schedule="@daily",
    catchup=False,
    tags=["jobs", "etl", "portfolio"],
    description="A DAG to extract, transform, and load tech job data into S3 and a database.",
)
def tech_jobs_pipeline():

    @task
    def extract_and_save_raw_jobs_task():
        """Extract job data from the API."""
        from src.extract.jobs_api import fetch_jobs
        from src.utils.s3_utils import save_raw_jobs_s3

        jobs = fetch_jobs()
        if not jobs:
            raise ValueError("No jobs were fetched from the API.")
        
        partition = build_partition_from_context()

        path = save_raw_jobs_s3(
            jobs=jobs,
            bucket_name="my-airflow-jobs-bucket",
            partition=partition
        )

        print(f"Raw save to {path}")
        return path

    @task
    def transform_and_save_processed_task(raw_s3_path):
        """Transform raw job data into a structured format."""
        from src.utils.s3_utils import read_jobs_from_s3, save_processed_jobs_s3
        from src.transform.jobs_transformer import transform_jobs

        raw_jobs = read_jobs_from_s3(raw_s3_path)
        clean_jobs = transform_jobs(raw_jobs)

        partition = build_partition_from_context()

        path = save_processed_jobs_s3(
            jobs=clean_jobs,
            bucket_name="my-airflow-jobs-bucket",
            partition=partition
        )

        print(f"Processed save to {path}")
        return path

    @task
    def load_jobs_task(processed_s3_path):
        """Load the processed job data into the database."""
        from src.utils.s3_utils import read_jobs_from_s3
        from src.load.postgres_loader import load_jobs

        clean_jobs = read_jobs_from_s3(processed_s3_path)
        print(f"Loading {len(clean_jobs)} jobs into the database")
        load_jobs(clean_jobs)

    # Setting up task dependencies
    raw_s3_path = extract_and_save_raw_jobs_task()
    processed_s3_path = transform_and_save_processed_task(raw_s3_path)
    load = load_jobs_task(processed_s3_path)
    
# Instantiate the DAG
tech_jobs_pipeline()