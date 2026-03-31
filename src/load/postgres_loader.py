import logging

from psycopg2.extras import execute_values

from src.utils.db import get_db_connection
from src.schemas.job_schema import CleanJob


logger = logging.getLogger(__name__)

# Extract fields from schema
COLUMNS = tuple(CleanJob.__annotations__.keys()
                )

def load_jobs(clean_jobs: list[dict]):
    """Load cleaned jobs into PostgreSQL.
    
    Args:
        clean_jobs: List of cleaned job dictionaries conforming to CleanJob schema.
        
    Returns:
        Number of rows inserted.
    """
    if not clean_jobs:
        logger.info("No jobs to load into the database.")
        return 0
    
    rows = [tuple(job.get(col) for col in COLUMNS) for job in clean_jobs]

    insert_query = f"""
    INSERT INTO jobs_clean ({", ".join(COLUMNS)})
    VALUES %s
    ON CONFLICT (job_url) DO NOTHING
    RETURNING job_url;
    """

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        execute_values(cursor, insert_query, rows)
        inserted_rows = len(cursor.fetchall())
        conn.commit()

        logger.info("Loaded %d jobs into jobs_clean table.", inserted_rows)
        return inserted_rows
    except Exception:
        conn.rollback()
        logger.exception("Failed to load jobs into the database.")
        raise
    finally:
        cursor.close()
        conn.close()