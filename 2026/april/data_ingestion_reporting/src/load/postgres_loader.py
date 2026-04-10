from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from utils.db import get_connection


SOURCE_ID = 1


def load_jobs(jobs: list[dict[str, Any]]) -> int:
    """
    Insert validated jobs into Postgres.
    Returns the number of newly inserted rows.
    """
    inserted_count = 0
    conn = get_connection()
    cur = conn.cursor()

    try:
        for job in jobs:
            cur.execute(
                """
                INSERT INTO jobs (
                    source_id,
                    source_job_id,
                    title,
                    company,
                    location,
                    salary,
                    description,
                    url,
                    posted_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source_id, source_job_id) DO NOTHING
                """,
                (
                    SOURCE_ID,
                    job["source_job_id"],
                    job["title"],
                    job.get("company"),
                    job.get("location"),
                    job.get("salary"),
                    job.get("description"),
                    job["url"],
                    job.get("posted_at"),
                ),
            )

            if cur.rowcount > 0:
                inserted_count += 1

        conn.commit()
        logging.info("Inserted %s new jobs.", inserted_count)
        return inserted_count

    except Exception:
        conn.rollback()
        logging.exception("Failed while loading jobs into Postgres.")
        raise

    finally:
        cur.close()
        conn.close()


def log_ingestion_run(
    status: str,
    records_loaded: int,
    errors: str | None = None,
) -> None:
    """
    Log a pipeline run in the ingestion_runs table.
    """
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            INSERT INTO ingestion_runs (run_time, status, records_loaded, errors)
            VALUES (%s, %s, %s, %s)
            """,
            (datetime.now(), status, records_loaded, errors),
        )
        conn.commit()

    except Exception:
        conn.rollback()
        logging.exception("Failed to log ingestion run.")

    finally:
        cur.close()
        conn.close()