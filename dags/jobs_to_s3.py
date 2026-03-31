from airflow.sdk import dag, task, get_current_context
from pendulum import datetime
import json
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


@dag(
    dag_id="jobs_to_s3",
    start_date=datetime(2026, 3, 31, tz="Europe/Madrid"),
    schedule=None,
    catchup=False,
    description="Extracts sample job data, transforms it, and uploads it to S3 using temporary AWS credentials.",
    tags=["aws", "s3", "airflow", "portfolio"],
)
def jobs_to_s3():

    @task
    def extract():
        data = [
            {"id": 1, "title": "Data Engineer", "location": "Madrid"},
            {"id": 2, "title": "Cloud Engineer", "location": "Remote"},
        ]
        print(f"Extracted {len(data)} records")
        return data

    @task
    def transform(records: list[dict]):
        transformed = []
        for record in records:
            transformed.append(
                {
                    "job_id": record["id"],
                    "job_title": record["title"].upper(),
                    "location": record["location"],
                }
            )
        print(f"Transformed {len(transformed)} records")
        return transformed

    @task
    def upload_to_s3(records: list[dict]):

        run_date = get_current_context()["data_interval_start"].format("DD-MM-YYYY")

        key = f"raw/jobs/{run_date}/jobs.json"

        hook = S3Hook(aws_conn_id=None)

        payload = json.dumps(records, indent=2, ensure_ascii=False)

        hook.load_string(
            string_data=payload,
            key=key,
            bucket_name="my-airflow-jobs-bucket",
            replace=True,
        )

        print(f"File uploaded to S3: s3://my-airflow-jobs-bucket/{key}")

    upload_to_s3(transform(extract()))

jobs_to_s3()