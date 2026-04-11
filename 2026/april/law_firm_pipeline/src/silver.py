from __future__ import annotations

"""
Silver layer for the law firm medallion pipeline.

Silver is the cleaned and standardized layer.

What happens in Silver:
- parse dates
- standardize lead sources
- standardize channels
- remove duplicates
- clean nulls
- align related tables to a more consistent structure
- unify calls, SMS, and emails into one communications table

What does NOT happen here:
- no executive KPI calculations
- no dashboard-specific aggregations

Silver should be trusted, analysis-ready data.
Gold will build business-facing outputs from this layer.
"""

from typing import Dict
import pandas as pd

from utils import (
    BRONZE_DIR,
    SILVER_DIR,
    read_csv,
    write_csv,
    safe_to_datetime,
    standardize_lead_source,
    standardize_channel,
    logger,
)


def load_bronze_tables() -> Dict[str, pd.DataFrame]:
    """
    Load Bronze layer CSV files.

    Returns
    -------
    dict[str, pd.DataFrame]
    """
    tables = {}
    for table_name in [
        "leads",
        "matters",
        "calls",
        "sms",
        "emails",
        "marketing_spend",
        "payments",
    ]:
        tables[table_name] = read_csv(BRONZE_DIR / f"{table_name}.csv")
    return tables


def clean_leads(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize lead data.

    Expected columns
    ----------------
    lead_id, created_at, lead_source, practice_area, assigned_to, status

    Returns
    -------
    pd.DataFrame
    """
    cleaned = df.copy()
    cleaned = safe_to_datetime(cleaned, ["created_at", "ingested_at"])

    if "lead_source" in cleaned.columns:
        cleaned["lead_source_standardized"] = cleaned["lead_source"].apply(standardize_lead_source)
    else:
        cleaned["lead_source_standardized"] = "Unknown"

    if "practice_area" in cleaned.columns:
        cleaned["practice_area"] = cleaned["practice_area"].fillna("Unknown").astype(str).str.strip()

    if "assigned_to" in cleaned.columns:
        cleaned["assigned_to"] = cleaned["assigned_to"].fillna("Unassigned").astype(str).str.strip()

    if "status" in cleaned.columns:
        cleaned["status"] = cleaned["status"].fillna("Unknown").astype(str).str.strip()

    if "lead_id" in cleaned.columns:
        cleaned = cleaned.drop_duplicates(subset=["lead_id"], keep="last")

    return cleaned


def clean_matters(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize matter or case data.

    Expected columns
    ----------------
    matter_id, lead_id, opened_at, consultation_at, retained_flag, practice_area,
    attorney, status

    Returns
    -------
    pd.DataFrame
    """
    cleaned = df.copy()
    cleaned = safe_to_datetime(cleaned, ["opened_at", "consultation_at", "ingested_at"])

    if "retained_flag" in cleaned.columns:
        cleaned["retained_flag"] = (
            cleaned["retained_flag"]
            .astype(str)
            .str.strip()
            .str.lower()
            .isin(["true", "1", "yes", "y"])
            .astype(int)
        )
    else:
        cleaned["retained_flag"] = 0

    if "practice_area" in cleaned.columns:
        cleaned["practice_area"] = cleaned["practice_area"].fillna("Unknown").astype(str).str.strip()

    if "attorney" in cleaned.columns:
        cleaned["attorney"] = cleaned["attorney"].fillna("Unknown").astype(str).str.strip()

    if "status" in cleaned.columns:
        cleaned["status"] = cleaned["status"].fillna("Unknown").astype(str).str.strip()

    if "matter_id" in cleaned.columns:
        cleaned = cleaned.drop_duplicates(subset=["matter_id"], keep="last")

    return cleaned


def _clean_communication_base(
    df: pd.DataFrame,
    channel_name: str,
    id_column: str,
    time_column: str,
) -> pd.DataFrame:
    """
    Standardize one communication table into a common structure.

    Parameters
    ----------
    df:
        Source communication DataFrame.
    channel_name:
        Fixed channel label for the source table.
    id_column:
        Primary ID column for that source.
    time_column:
        Timestamp column for the event.

    Returns
    -------
    pd.DataFrame
    """
    cleaned = df.copy()

    if time_column in cleaned.columns:
        cleaned = safe_to_datetime(cleaned, [time_column, "ingested_at"])

    output = pd.DataFrame()

    output["communication_id"] = cleaned[id_column] if id_column in cleaned.columns else range(1, len(cleaned) + 1)
    output["lead_id"] = cleaned["lead_id"] if "lead_id" in cleaned.columns else None
    output["matter_id"] = cleaned["matter_id"] if "matter_id" in cleaned.columns else None
    output["event_time"] = cleaned[time_column] if time_column in cleaned.columns else pd.NaT
    output["direction"] = cleaned["direction"] if "direction" in cleaned.columns else "Unknown"
    output["handled_by"] = cleaned["handled_by"] if "handled_by" in cleaned.columns else "Unknown"
    output["channel"] = channel_name
    output["ingested_at"] = cleaned["ingested_at"] if "ingested_at" in cleaned.columns else pd.Timestamp.utcnow()

    output["direction"] = output["direction"].fillna("Unknown").astype(str).str.strip().str.title()
    output["handled_by"] = output["handled_by"].fillna("Unknown").astype(str).str.strip()
    output["channel"] = output["channel"].apply(standardize_channel)

    return output


def build_communications_table(
    calls: pd.DataFrame,
    sms: pd.DataFrame,
    emails: pd.DataFrame,
) -> pd.DataFrame:
    """
    Combine calls, SMS, and emails into one unified Silver communications table.

    Returns
    -------
    pd.DataFrame
    """
    calls_clean = _clean_communication_base(calls, "Call", "call_id", "call_time")
    sms_clean = _clean_communication_base(sms, "SMS", "sms_id", "sent_at")
    emails_clean = _clean_communication_base(emails, "Email", "email_id", "sent_at")

    communications = pd.concat([calls_clean, sms_clean, emails_clean], ignore_index=True)
    communications = communications.drop_duplicates(subset=["communication_id", "channel"], keep="last")
    return communications


def clean_marketing_spend(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean marketing spend table.

    Expected columns
    ----------------
    month, lead_source, spend

    Returns
    -------
    pd.DataFrame
    """
    cleaned = df.copy()
    cleaned = safe_to_datetime(cleaned, ["month", "ingested_at"])

    if "lead_source" in cleaned.columns:
        cleaned["lead_source_standardized"] = cleaned["lead_source"].apply(standardize_lead_source)
    else:
        cleaned["lead_source_standardized"] = "Unknown"

    if "spend" in cleaned.columns:
        cleaned["spend"] = pd.to_numeric(cleaned["spend"], errors="coerce").fillna(0.0)

    return cleaned


def clean_payments(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean payments or revenue table.

    Expected columns
    ----------------
    payment_id, matter_id, payment_date, amount

    Returns
    -------
    pd.DataFrame
    """
    cleaned = df.copy()
    cleaned = safe_to_datetime(cleaned, ["payment_date", "ingested_at"])

    if "amount" in cleaned.columns:
        cleaned["amount"] = pd.to_numeric(cleaned["amount"], errors="coerce").fillna(0.0)

    if "payment_id" in cleaned.columns:
        cleaned = cleaned.drop_duplicates(subset=["payment_id"], keep="last")

    return cleaned


def build_silver_layer() -> Dict[str, pd.DataFrame]:
    """
    Build all Silver tables from Bronze inputs.

    Returns
    -------
    dict[str, pd.DataFrame]
    """
    bronze = load_bronze_tables()

    silver_tables = {
        "leads_clean": clean_leads(bronze["leads"]),
        "matters_clean": clean_matters(bronze["matters"]),
        "communications_clean": build_communications_table(
            bronze["calls"], bronze["sms"], bronze["emails"]
        ),
        "marketing_spend_clean": clean_marketing_spend(bronze["marketing_spend"]),
        "payments_clean": clean_payments(bronze["payments"]),
    }

    for table_name, df in silver_tables.items():
        write_csv(df, SILVER_DIR / f"{table_name}.csv")

    logger.info("Silver layer complete.")
    return silver_tables


if __name__ == "__main__":
    build_silver_layer()