import json
import boto3
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from typing import Any

def save_jobs_to_s3(
        jobs: list[dict[str, Any]],
        bucket_name: str,
        key:str,
        aws_conn_id: str | None = None,
) -> str:
    """Save job data to S3 as a JSON file and return the S3 path."""

    payload = json.dumps(jobs, indent=2, ensure_ascii=False)

    if aws_conn_id:
        hook = S3Hook(aws_conn_id=aws_conn_id)
    else:
        hook = S3Hook()

    hook.load_string(
        string_data=payload,
        key=key,
        bucket_name=bucket_name,
        replace=True,
    )
    
    return f"s3://{bucket_name}/{key}"

    
def save_raw_jobs_s3(
        jobs: list[dict[str, Any]],
        bucket_name: str,
        partition: str,
) -> str:
    """Saves the raw job data to S3 as a JSON file."""
    key = f"raw/jobs/{partition}/jobs.json"
    return save_jobs_to_s3(jobs=jobs, bucket_name= bucket_name, key=key, aws_conn_id=None)

def save_processed_jobs_s3(
        jobs: list[dict[str, Any]],
        bucket_name: str,
        partition: str,
) -> str:
    """Saves the processed job data to S3 as a JSON file."""
    key = f"processed/jobs/{partition}/jobs.json"
    return save_jobs_to_s3(jobs=jobs, bucket_name=bucket_name, key=key, aws_conn_id=None)


def read_jobs_from_s3(
        s3_path:str, 
        aws_conn_id: str | None = None,
) -> list[dict[str, Any]]:
    """Read a JSON jobs file from S3 and return its contents."""
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Invalid S3 path: {s3_path}")

    path_without_scheme = s3_path.removeprefix("s3://")
    bucket_name, key = path_without_scheme.split("/", 1)

    print(f"Reading from S3 bucket '{bucket_name}' at key '{key}'")

    if aws_conn_id:
        hook = S3Hook(aws_conn_id=aws_conn_id)
    else:
        hook = S3Hook()

    content = hook.read_key(key=key, bucket_name=bucket_name)
    if not content:
        raise ValueError(f"Empty content in {s3_path}")
    
    data = json.loads(content)

    if not isinstance(data, list):
        raise ValueError(f"Expected a list of jobs in {s3_path}, got {type(data).__name__}")

    return data
