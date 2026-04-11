from __future__ import annotations

"""
Shared utility functions for the law firm medallion pipeline.

This module contains small reusable helpers used across Bronze, Silver,
Gold, and Quality layers.

Why this file exists:
- avoids repeating file and date handling logic
- keeps the pipeline modules focused on their own responsibility
- gives us one place to maintain shared rules such as standardizing names
"""

from pathlib import Path
from typing import Iterable
import logging

import pandas as pd

# -----------------------------------------------------------------------------
# Logging configuration
# -----------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger("law_firm_pipeline")

# -----------------------------------------------------------------------------
# Project paths
# -----------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"

SOURCE_DIR = DATA_DIR / "source"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

# -----------------------------------------------------------------------------
# Category mapping rules
# -----------------------------------------------------------------------------

LEAD_SOURCE_MAP = {
    "google ads": "Google Ads",
    "google_adwords": "Google Ads",
    "paid search": "Google Ads",
    "google": "Google Ads",
    "facebook": "Facebook",
    "facebook ads": "Facebook",
    "meta ads": "Facebook",
    "instagram": "Facebook",
    "referral": "Referral",
    "word of mouth": "Referral",
    "friend referral": "Referral",
    "website": "Website",
    "organic website": "Website",
    "organic search": "Website",
    "seo": "Website",
    "unknown": "Unknown",
    "": "Unknown",
}

CHANNEL_MAP = {
    "call": "Call",
    "phone": "Call",
    "sms": "SMS",
    "text": "SMS",
    "email": "Email",
    "e-mail": "Email",
}

# -----------------------------------------------------------------------------
# File system helpers
# -----------------------------------------------------------------------------

def ensure_directories() -> None:
    """
    Create all required data directories if they do not already exist.

    This keeps the pipeline safe to run on a fresh repo.
    """
    for path in [SOURCE_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR]:
        path.mkdir(parents=True, exist_ok=True)


def write_csv(df: pd.DataFrame, path: Path) -> None:
    """
    Write a DataFrame to CSV after making sure the parent folder exists.

    Parameters
    ----------
    df:
        The pandas DataFrame to save.
    path:
        Full output path.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    logger.info("Wrote file: %s", path)


def read_csv(path: Path) -> pd.DataFrame:
    """
    Read a CSV file into a DataFrame.

    Parameters
    ----------
    path:
        Full path to the CSV file.

    Returns
    -------
    pd.DataFrame
    """
    logger.info("Reading file: %s", path)
    return pd.read_csv(path)


# -----------------------------------------------------------------------------
# Data cleaning helpers
# -----------------------------------------------------------------------------

def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert DataFrame column names to lowercase snake_case.

    Example:
        'Lead Source' -> 'lead_source'

    Parameters
    ----------
    df:
        Input DataFrame.

    Returns
    -------
    pd.DataFrame
        DataFrame with cleaned column names.
    """
    renamed = df.copy()
    renamed.columns = [
        str(col).strip().lower().replace(" ", "_").replace("-", "_")
        for col in renamed.columns
    ]
    return renamed


def safe_to_datetime(
    df: pd.DataFrame,
    columns: Iterable[str],
) -> pd.DataFrame:
    """
    Safely convert the given columns to pandas datetime.

    Invalid values become NaT rather than crashing the pipeline.

    Parameters
    ----------
    df:
        Input DataFrame.
    columns:
        Column names to parse.

    Returns
    -------
    pd.DataFrame
    """
    converted = df.copy()
    for col in columns:
        if col in converted.columns:
            converted[col] = pd.to_datetime(converted[col], errors="coerce")
    return converted


def normalize_text(value: object) -> str:
    """
    Normalize a text value for mapping and matching.

    Parameters
    ----------
    value:
        Any incoming value.

    Returns
    -------
    str
        Lowercased, stripped string.
    """
    if pd.isna(value):
        return ""
    return str(value).strip().lower()


def standardize_lead_source(value: object) -> str:
    """
    Convert a raw lead source into a standardized label.

    Parameters
    ----------
    value:
        Raw source text.

    Returns
    -------
    str
        Standardized lead source.
    """
    normalized = normalize_text(value)
    return LEAD_SOURCE_MAP.get(normalized, str(value).strip() if str(value).strip() else "Unknown")


def standardize_channel(value: object) -> str:
    """
    Convert a raw communication channel into a standardized label.

    Parameters
    ----------
    value:
        Raw channel text.

    Returns
    -------
    str
        Standardized channel.
    """
    normalized = normalize_text(value)
    return CHANNEL_MAP.get(normalized, str(value).strip().title() if str(value).strip() else "Unknown")


def null_safe_divide(numerator: float, denominator: float) -> float:
    """
    Perform division safely.

    Returns 0.0 when denominator is 0 or missing.

    Parameters
    ----------
    numerator:
        Top value.
    denominator:
        Bottom value.

    Returns
    -------
    float
    """
    if denominator in (0, None) or pd.isna(denominator):
        return 0.0
    return float(numerator) / float(denominator)