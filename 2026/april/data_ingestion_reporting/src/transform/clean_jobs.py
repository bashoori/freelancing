from __future__ import annotations

import html
import logging
from datetime import datetime
from typing import Any


def clean_jobs(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Normalize, clean, and validate raw API job records.
    """
    cleaned_jobs: list[dict[str, Any]] = []

    # RemoteOK usually returns metadata in the first item
    for item in data[1:]:
        job = {
            "source_job_id": _clean_text(item.get("id")),
            "title": _clean_text(item.get("position")),
            "company": _clean_text(item.get("company")),
            "location": _clean_text(item.get("location")),
            "salary": _build_salary(item),
            "description": _clean_text(item.get("description")),
            "url": _clean_text(item.get("url")),
            "posted_at": _parse_posted_at(item.get("epoch")),
        }

        if _is_valid(job):
            cleaned_jobs.append(job)

    return cleaned_jobs


def _clean_text(value: Any) -> str | None:
    """
    Convert value to string, strip whitespace, and decode HTML entities.
    Returns None for empty values.
    """
    if value is None:
        return None

    text = html.unescape(str(value)).strip()
    return text if text else None


def _parse_posted_at(epoch_value: Any) -> datetime | None:
    """
    Convert epoch timestamp to datetime.
    Returns None if parsing fails.
    """
    if not epoch_value:
        return None

    try:
        return datetime.fromtimestamp(int(epoch_value))
    except (ValueError, TypeError):
        logging.warning("Invalid epoch value: %s", epoch_value)
        return None


def _is_valid(job: dict[str, Any]) -> bool:
    """
    Ensure required fields exist before loading.
    """
    required_fields = ["source_job_id", "title", "url"]

    for field in required_fields:
        if not job.get(field):
            logging.warning("Skipping invalid record. Missing %s | %s", field, job)
            return False

    return True


def _build_salary(item: dict[str, Any]) -> str | None:
    """
    Build a simple salary string from available min/max values.
    """
    salary_min = item.get("salary_min")
    salary_max = item.get("salary_max")

    if salary_min and salary_max:
        return f"{salary_min}-{salary_max}"
    if salary_min:
        return str(salary_min)
    if salary_max:
        return str(salary_max)

    return None