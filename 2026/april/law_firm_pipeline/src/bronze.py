from __future__ import annotations

"""
Bronze layer for the law firm medallion pipeline.

Bronze is the raw landing zone.
The goal is to preserve source data with minimal transformation.

What happens in Bronze:
- read source CSV files
- standardize column names only
- add ingestion metadata
- write raw tables into data/bronze

What does NOT happen here:
- no business logic
- no category mapping
- no deduplication
- no reporting calculations

This keeps Bronze as the closest version of the original source data.
"""

from pathlib import Path
from typing import Dict
import pandas as pd

from utils import (
    SOURCE_DIR,
    BRONZE_DIR,
    ensure_directories,
    read_csv,
    write_csv,
    standardize_column_names,
    logger,
)

SOURCE_FILES = {
    "leads": "leads.csv",
    "matters": "matters.csv",
    "calls": "calls.csv",
    "sms": "sms.csv",
    "emails": "emails.csv",
    "marketing_spend": "marketing_spend.csv",
    "payments": "payments.csv",
}


def load_source_table(file_name: str) -> pd.DataFrame:
    """
    Load one source CSV file.

    Parameters
    ----------
    file_name:
        Name of the CSV file inside data/source.

    Returns
    -------
    pd.DataFrame
    """
    path = SOURCE_DIR / file_name
    df = read_csv(path)
    df = standardize_column_names(df)
    df["ingested_at"] = pd.Timestamp.utcnow()
    return df


def build_bronze_layer() -> Dict[str, pd.DataFrame]:
    """
    Build all Bronze tables from source files.

    Returns
    -------
    dict[str, pd.DataFrame]
        A dictionary of Bronze DataFrames keyed by logical table name.
    """
    ensure_directories()

    bronze_tables: Dict[str, pd.DataFrame] = {}

    for table_name, file_name in SOURCE_FILES.items():
        logger.info("Building Bronze table: %s", table_name)
        df = load_source_table(file_name)
        bronze_tables[table_name] = df

        output_path = BRONZE_DIR / f"{table_name}.csv"
        write_csv(df, output_path)

    return bronze_tables


if __name__ == "__main__":
    build_bronze_layer()