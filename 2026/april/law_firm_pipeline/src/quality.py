from __future__ import annotations

"""
Data quality checks for the law firm medallion pipeline.

This module creates simple, explainable checks that help us trust
the Silver and Gold layers.

What this module checks:
- required columns
- null counts in important keys
- duplicate key counts
- negative numeric values
- invalid date order rules

Output:
- a DataFrame with one row per quality rule
- written to data/gold/quality_report.csv by the pipeline
"""

from typing import Iterable, List, Dict
import pandas as pd

from utils import null_safe_divide


def check_required_columns(
    df: pd.DataFrame,
    table_name: str,
    required_columns: Iterable[str],
) -> List[Dict[str, object]]:
    """
    Check whether expected columns exist in a table.
    """
    results: List[Dict[str, object]] = []

    for col in required_columns:
        passed = col in df.columns
        results.append(
            {
                "table_name": table_name,
                "check_type": "required_column",
                "check_name": f"column_exists:{col}",
                "status": "PASS" if passed else "FAIL",
                "issue_count": 0 if passed else 1,
            }
        )

    return results


def check_nulls(
    df: pd.DataFrame,
    table_name: str,
    key_columns: Iterable[str],
) -> List[Dict[str, object]]:
    """
    Check null counts for important columns.
    """
    results: List[Dict[str, object]] = []

    for col in key_columns:
        if col not in df.columns:
            continue

        null_count = int(df[col].isna().sum())
        results.append(
            {
                "table_name": table_name,
                "check_type": "null_check",
                "check_name": f"nulls_in:{col}",
                "status": "PASS" if null_count == 0 else "WARN",
                "issue_count": null_count,
            }
        )

    return results


def check_duplicates(
    df: pd.DataFrame,
    table_name: str,
    subset: Iterable[str],
    label: str,
) -> List[Dict[str, object]]:
    """
    Check duplicate rows by key subset.
    """
    subset_list = list(subset)
    if not all(col in df.columns for col in subset_list):
        return []

    duplicate_count = int(df.duplicated(subset=subset_list).sum())

    return [
        {
            "table_name": table_name,
            "check_type": "duplicate_check",
            "check_name": f"duplicates_in:{label}",
            "status": "PASS" if duplicate_count == 0 else "WARN",
            "issue_count": duplicate_count,
        }
    ]


def check_negative_values(
    df: pd.DataFrame,
    table_name: str,
    columns: Iterable[str],
) -> List[Dict[str, object]]:
    """
    Check whether numeric columns contain negative values.
    """
    results: List[Dict[str, object]] = []

    for col in columns:
        if col not in df.columns:
            continue

        negative_count = int((pd.to_numeric(df[col], errors="coerce").fillna(0) < 0).sum())
        results.append(
            {
                "table_name": table_name,
                "check_type": "negative_value_check",
                "check_name": f"negative_values_in:{col}",
                "status": "PASS" if negative_count == 0 else "WARN",
                "issue_count": negative_count,
            }
        )

    return results


def check_date_order(
    df: pd.DataFrame,
    table_name: str,
    earlier_col: str,
    later_col: str,
) -> List[Dict[str, object]]:
    """
    Check that an earlier date is not after a later date.

    Example:
        created_at should not be after consultation_at
    """
    if earlier_col not in df.columns or later_col not in df.columns:
        return []

    earlier = pd.to_datetime(df[earlier_col], errors="coerce")
    later = pd.to_datetime(df[later_col], errors="coerce")

    invalid_count = int(((earlier.notna()) & (later.notna()) & (earlier > later)).sum())

    return [
        {
            "table_name": table_name,
            "check_type": "date_order_check",
            "check_name": f"{earlier_col}_before_{later_col}",
            "status": "PASS" if invalid_count == 0 else "WARN",
            "issue_count": invalid_count,
        }
    ]


def summarize_quality(results: List[Dict[str, object]]) -> pd.DataFrame:
    """
    Convert quality rule output into a DataFrame and add summary fields.
    """
    report = pd.DataFrame(results)

    if report.empty:
        return pd.DataFrame(
            columns=["table_name", "check_type", "check_name", "status", "issue_count"]
        )

    total_checks = len(report)
    passed_checks = int((report["status"] == "PASS").sum())
    pass_rate = null_safe_divide(passed_checks, total_checks)

    report["total_checks_run"] = total_checks
    report["pass_rate"] = round(pass_rate, 4)

    return report


def run_silver_quality_checks(silver_tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Run quality checks for the Silver layer.

    Returns
    -------
    pd.DataFrame
        Quality report.
    """
    results: List[Dict[str, object]] = []

    leads = silver_tables["leads_clean"]
    matters = silver_tables["matters_clean"]
    communications = silver_tables["communications_clean"]
    marketing = silver_tables["marketing_spend_clean"]
    payments = silver_tables["payments_clean"]

    results.extend(check_required_columns(leads, "leads_clean", ["lead_id", "created_at"]))
    results.extend(check_nulls(leads, "leads_clean", ["lead_id"]))
    results.extend(check_duplicates(leads, "leads_clean", ["lead_id"], "lead_id"))

    results.extend(check_required_columns(matters, "matters_clean", ["matter_id", "lead_id"]))
    results.extend(check_nulls(matters, "matters_clean", ["matter_id", "lead_id"]))
    results.extend(check_duplicates(matters, "matters_clean", ["matter_id"], "matter_id"))
    results.extend(check_date_order(matters, "matters_clean", "consultation_at", "opened_at"))

    results.extend(check_required_columns(communications, "communications_clean", ["communication_id", "channel"]))
    results.extend(check_nulls(communications, "communications_clean", ["communication_id"]))
    results.extend(check_duplicates(communications, "communications_clean", ["communication_id", "channel"], "communication_id_channel"))

    results.extend(check_required_columns(marketing, "marketing_spend_clean", ["month", "spend"]))
    results.extend(check_negative_values(marketing, "marketing_spend_clean", ["spend"]))

    results.extend(check_required_columns(payments, "payments_clean", ["payment_id", "matter_id", "amount"]))
    results.extend(check_duplicates(payments, "payments_clean", ["payment_id"], "payment_id"))
    results.extend(check_negative_values(payments, "payments_clean", ["amount"]))

    return summarize_quality(results)