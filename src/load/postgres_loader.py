import logging
from itertools import islice
from typing import Any, Iterable

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

from src.schemas.job_schema import CleanJob


logger = logging.getLogger(__name__)

# Explicitly define which columns go to the database to avoid surprises.
COLUMNS = tuple(CleanJob.__annotations__.keys())
CONFLICT_COLUMN = "job_url"
BATCH_SIZE = 5000
PAGE_SIZE = 1000

# Columns that must always be present (NOT NULL in schema or conflict key).
REQUIRED_COLUMNS = {"title", "job_url"}

def chunked(iterable: Iterable[dict[str, Any]], size: int):
    """Yield successive chunks of the iterable."""
    it = iter(iterable)
    while True:
        batch = list(islice(it, size))
        if not batch:
            return
        yield batch


def _validate_and_prepare_row(job: dict[str, Any]) -> tuple:
    """
    Validate that a job has all required columns.
    Replace missing nullable values with None, allowing NULLs in the database.
    Raise ValueError if required fields are missing.
    """
    # Check required columns first.
    missing_required = REQUIRED_COLUMNS - set(job.keys())
    if missing_required:
        raise ValueError(
            f"Missing required columns: {missing_required}. "
            f"Available: {list(job.keys())}"
        )
    
    # Build row with all COLUMNS, filling missing nullable ones with None.
    row = []
    for col in COLUMNS:
        if col not in job:
            row.append(None)
            logger.debug(f"Column '{col}' missing in job, using NULL.")
        else:
            row.append(job[col])
    return tuple(row)


def load_jobs(clean_jobs: list[dict[str, Any]]) -> int:
    """
    Load cleaned jobs into PostgreSQL and return affected rows.
    Uses UPSERT (INSERT ... ON CONFLICT) to insert new jobs or update existing ones.
    Only updates rows if data has actually changed (IS DISTINCT FROM predicate).
    """
    if not clean_jobs:
        logger.info("No jobs to load into the database.")
        return 0

    # Build UPDATE assignments for all columns except the conflict key.
    update_columns = [col for col in COLUMNS if col != CONFLICT_COLUMN]
    update_assignments = [f"{col} = EXCLUDED.{col}" for col in update_columns]
    update_assignments.append("updated_at = CURRENT_TIMESTAMP")
    update_set_clause = ", ".join(update_assignments)

    # Avoid unnecessary updates when there are no real changes.
    changed_predicate = " OR ".join(
        f"jobs_clean.{col} IS DISTINCT FROM EXCLUDED.{col}"
        for col in update_columns
    )

    # CTE counts UPSERTs without bringing all rows to the client.
    # Much more efficient than RETURNING without aggregation.
    insert_query = f"""
    WITH upserted AS (
        INSERT INTO jobs_clean ({", ".join(COLUMNS)})
        VALUES %s
        ON CONFLICT ({CONFLICT_COLUMN}) DO UPDATE
        SET {update_set_clause}
        WHERE {changed_predicate}
        RETURNING 1
    )
    SELECT COUNT(*)::int FROM upserted;
    """

    hook = PostgresHook(postgres_conn_id="job_db")
    conn = hook.get_conn()

    total_affected = 0
    total_processed = 0

    try:
        with conn, conn.cursor() as cursor:
            for batch in chunked(clean_jobs, BATCH_SIZE):
                # Validate each row and prepare tuples.
                rows = [_validate_and_prepare_row(job) for job in batch]
                total_processed += len(batch)

                execute_values(cursor, insert_query, rows, page_size=PAGE_SIZE)
                affected_in_batch = cursor.fetchone()[0]
                total_affected += affected_in_batch

                logger.debug(
                    "Batch processed: %d jobs, %d affected rows.",
                    len(batch),
                    affected_in_batch,
                )

        logger.info(
            "Upsert completed. Jobs processed: %d, Affected rows: %d",
            total_processed,
            total_affected
        )
        return total_affected
    except Exception:
        logger.exception("Failed to load jobs into the database.")
        raise
    finally:
        conn.close()