from __future__ import annotations

import logging
import os

from extract.api_client import fetch_jobs
from load.postgres_loader import load_jobs, log_ingestion_run
from transform.clean_jobs import clean_jobs


os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


def run_pipeline() -> None:
    """
    Run the ETL pipeline:
    1. Extract raw data
    2. Transform and validate
    3. Load into Postgres
    4. Log pipeline result
    """
    status = "success"
    records_loaded = 0
    error_message = None

    try:
        raw_data = fetch_jobs()
        jobs = clean_jobs(raw_data)
        records_loaded = load_jobs(jobs)

        logging.info("Pipeline completed successfully. Inserted %s jobs.", records_loaded)
        print(f"Pipeline complete. Inserted {records_loaded} new jobs.")

    except Exception as exc:
        status = "failed"
        error_message = str(exc)
        logging.exception("Pipeline failed.")
        print(f"Pipeline failed: {error_message}")

    finally:
        log_ingestion_run(
            status=status,
            records_loaded=records_loaded,
            errors=error_message,
        )


if __name__ == "__main__":
    run_pipeline()