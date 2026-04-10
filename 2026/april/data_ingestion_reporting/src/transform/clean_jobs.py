from __future__ import annotations

import logging
from datetime import datetime
from typing import Any


def clean_jobs(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Normalize and validate raw API records.
    """
    jobs: list[dict[str, Any]] = []

    # RemoteOK usually returns metadata in the first item
    for item in data[1:]:
        epoch_value = item.get("epoch")
        posted_at = None

        if epoch_value:
            try:
                posted_at = datetime.fromtimestamp(int(epoch_value))
            except (ValueError, TypeError):
                posted_at = None

        job = {
            "source_job_id": str(item.get("id", "")).strip(),
            "title": item.get("position"),
            "company": item.get("company"),
            "location": item.get("location"),
            "salary": _build_salary(item),
            "description": item.get("description"),
            "url": item.get("url"),
            "posted_at": posted_at,
        }

        if _is_valid(job):
            jobs.append(job)

    return jobs


def _is_valid(job: dict[str, Any]) -> bool:
    required_fields = ["source_job_id", "title", "url"]

    for field in required_fields:
        value = job.get(field)
        if value is None or str(value).strip() == "":
            logging.warning("Skipping invalid record. Missing %s | %s", field, job)
            return False

    return True


def _build_salary(item: dict[str, Any]) -> str | None:
    salary_min = item.get("salary_min")
    salary_max = item.get("salary_max")

    if salary_min and salary_max:
        return f"{salary_min}-{salary_max}"
    if salary_min:
        return str(salary_min)
    if salary_max:
        return str(salary_max)

    return None