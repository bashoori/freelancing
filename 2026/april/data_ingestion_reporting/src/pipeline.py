"""
ETL pipeline for ingesting job postings from RemoteOK into Postgres.

Flow:
1. Extract data from API
2. Normalize fields
3. Validate records
4. Deduplicate
5. Load into Postgres
6. Log run
"""

from __future__ import annotations

import os
import logging
from datetime import datetime
from typing import Any

import psycopg2
import requests
from dotenv import load_dotenv


load_dotenv()

API_URL = "https://remoteok.com/api"

logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


def extract() -> list[dict[str, Any]]:
    """Fetch raw job data from the API."""
    response = requests.get(
        API_URL,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=30,
    )
    response.raise_for_status()

    data = response.json()
    if not isinstance(data, list):
        raise ValueError("Unexpected API response format. Expected a list.")

    return data


def transform(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Normalize and validate raw job records."""
    jobs: list[dict[str, Any]] = []

    for item in data[1:]:
        epoch_value = item.get("epoch")
        posted_at = None

        if epoch_value:
            try:
                posted_at = datetime.fromtimestamp(int(epoch_value))
            except (ValueError, TypeError):
                posted_at = None

        job = {
            "source_job_id": str(item.get("id", "")),
            "title": item.get("position"),
            "company": item.get("company"),
            "location": item.get("location"),
            "salary": item.get("salary_min"),
            "description": item.get("description"),
            "url": item.get("url"),
            "posted_at": posted_at,
        }

        if validate_job(job):
            jobs.append(job)

    return jobs


def validate_job(job: dict[str, Any]) -> bool:
    """Check required fields before loading."""
    required_fields = ["source_job_id", "title", "url"]

    for field in required_fields:
        if not job.get(field):
            logging.warning("Skipping invalid record. Missing field: %s | %s", field, job)
            return False

    return True


def get_connection():
    """Create a Postgres connection using environment variables."""
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME", "jobs"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres"),
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
    )


def load(jobs: list[dict[str, Any]]) -> int:
    """Load validated jobs into Postgres and avoid duplicates."""
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
                    1,
                    job["source_job_id"],
                    job["title"],
                    job["company"],
                    job["location"],
                    job["salary"],
                    job["description"],
                    job["url"],
                    job["posted_at"],
                ),
            )

            if cur.rowcount > 0:
                inserted_count += 1

        conn.commit()
        return inserted_count

    except Exception:
        conn.rollback()
        raise

    finally:
        cur.close()
        conn.close()


def log_run(status: str, records_loaded: int, error_message: str | None = None) -> None:
    """Write pipeline run details to the ingestion_runs table."""
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            INSERT INTO ingestion_runs (run_time, status, records_loaded, errors)
            VALUES (%s, %s, %s, %s)
            """,
            (datetime.now(), status, records_loaded, error_message),
        )
        conn.commit()

    except Exception as exc:
        conn.rollback()
        logging.error("Failed to log ingestion run: %s", exc)

    finally:
        cur.close()
        conn.close()


def run_pipeline() -> None:
    """Run the end-to-end ETL pipeline."""
    status = "success"
    records_loaded = 0
    error_message = None

    try:
        data = extract()
        jobs = transform(data)
        records_loaded = load(jobs)

        logging.info("Pipeline completed successfully. Inserted %s jobs.", records_loaded)
        print(f"Pipeline complete. Inserted {records_loaded} new jobs.")

    except Exception as exc:
        status = "failed"
        error_message = str(exc)
        logging.exception("Pipeline failed.")
        print(f"Pipeline failed: {error_message}")

    finally:
        log_run(status=status, records_loaded=records_loaded, error_message=error_message)


if __name__ == "__main__":
    run_pipeline()