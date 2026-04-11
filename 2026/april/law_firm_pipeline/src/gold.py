from __future__ import annotations

"""
Gold layer for the law firm medallion pipeline.

Gold is the business-facing reporting layer.

What happens in Gold:
- join Silver tables
- compute KPI-ready aggregates
- produce stable reporting outputs for dashboards

Gold should answer business questions directly.
It should not preserve source messiness.
It should not contain raw system-specific quirks.
"""

from typing import Dict
import pandas as pd

from utils import SILVER_DIR, GOLD_DIR, read_csv, write_csv, null_safe_divide, logger


def load_silver_tables() -> Dict[str, pd.DataFrame]:
    """
    Load Silver tables from disk.

    Returns
    -------
    dict[str, pd.DataFrame]
    """
    return {
        "leads_clean": read_csv(SILVER_DIR / "leads_clean.csv"),
        "matters_clean": read_csv(SILVER_DIR / "matters_clean.csv"),
        "communications_clean": read_csv(SILVER_DIR / "communications_clean.csv"),
        "marketing_spend_clean": read_csv(SILVER_DIR / "marketing_spend_clean.csv"),
        "payments_clean": read_csv(SILVER_DIR / "payments_clean.csv"),
    }


def build_case_base(leads: pd.DataFrame, matters: pd.DataFrame) -> pd.DataFrame:
    """
    Create a case-centric analytical base table.

    This table combines lead information with matter information.

    Returns
    -------
    pd.DataFrame
    """
    case_base = matters.merge(
        leads[
            [
                col for col in [
                    "lead_id",
                    "created_at",
                    "lead_source_standardized",
                    "practice_area",
                    "assigned_to",
                    "status",
                ]
                if col in leads.columns
            ]
        ],
        on="lead_id",
        how="left",
        suffixes=("_matter", "_lead"),
    )

    case_base["opened_at"] = pd.to_datetime(case_base.get("opened_at"), errors="coerce")
    case_base["consultation_at"] = pd.to_datetime(case_base.get("consultation_at"), errors="coerce")
    case_base["created_at"] = pd.to_datetime(case_base.get("created_at"), errors="coerce")

    case_base["days_to_consult"] = (
        case_base["consultation_at"] - case_base["created_at"]
    ).dt.days

    case_base["days_to_open"] = (
        case_base["opened_at"] - case_base["created_at"]
    ).dt.days

    return case_base


def build_executive_monthly_kpis(
    leads: pd.DataFrame,
    case_base: pd.DataFrame,
    marketing: pd.DataFrame,
    payments: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build a monthly executive KPI summary.

    Metrics
    -------
    - leads
    - retained_cases
    - consultations
    - revenue
    - marketing_spend
    - retention_rate
    - cost_per_retained_case

    Returns
    -------
    pd.DataFrame
    """
    leads_copy = leads.copy()
    leads_copy["month"] = pd.to_datetime(leads_copy["created_at"], errors="coerce").dt.to_period("M").astype(str)

    lead_monthly = (
        leads_copy.groupby("month", dropna=False)
        .agg(leads=("lead_id", "nunique"))
        .reset_index()
    )

    cases_copy = case_base.copy()
    cases_copy["month"] = pd.to_datetime(cases_copy["opened_at"], errors="coerce").dt.to_period("M").astype(str)

    case_monthly = (
        cases_copy.groupby("month", dropna=False)
        .agg(
            consultations=("consultation_at", lambda x: x.notna().sum()),
            retained_cases=("retained_flag", "sum"),
        )
        .reset_index()
    )

    payments_copy = payments.copy()
    payments_copy["month"] = pd.to_datetime(payments_copy["payment_date"], errors="coerce").dt.to_period("M").astype(str)

    revenue_monthly = (
        payments_copy.groupby("month", dropna=False)
        .agg(revenue=("amount", "sum"))
        .reset_index()
    )

    marketing_copy = marketing.copy()
    marketing_copy["month"] = pd.to_datetime(marketing_copy["month"], errors="coerce").dt.to_period("M").astype(str)

    spend_monthly = (
        marketing_copy.groupby("month", dropna=False)
        .agg(marketing_spend=("spend", "sum"))
        .reset_index()
    )

    monthly = lead_monthly.merge(case_monthly, on="month", how="outer")
    monthly = monthly.merge(revenue_monthly, on="month", how="outer")
    monthly = monthly.merge(spend_monthly, on="month", how="outer")
    monthly = monthly.fillna(0)

    monthly["retention_rate"] = monthly.apply(
        lambda row: round(null_safe_divide(row["retained_cases"], row["leads"]), 4),
        axis=1,
    )
    monthly["cost_per_retained_case"] = monthly.apply(
        lambda row: round(null_safe_divide(row["marketing_spend"], row["retained_cases"]), 2),
        axis=1,
    )

    return monthly.sort_values("month")


def build_lead_source_performance(
    leads: pd.DataFrame,
    case_base: pd.DataFrame,
    marketing: pd.DataFrame,
    payments: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build source-level performance summary.

    Returns
    -------
    pd.DataFrame
    """
    case_source = case_base.copy()

    revenue_by_source = (
        case_source.merge(payments, on="matter_id", how="left")
        .groupby("lead_source_standardized", dropna=False)
        .agg(revenue=("amount", "sum"))
        .reset_index()
    )

    retained_by_source = (
        case_source.groupby("lead_source_standardized", dropna=False)
        .agg(
            retained_cases=("retained_flag", "sum"),
            avg_days_to_consult=("days_to_consult", "mean"),
        )
        .reset_index()
    )

    leads_by_source = (
        leads.groupby("lead_source_standardized", dropna=False)
        .agg(leads=("lead_id", "nunique"))
        .reset_index()
    )

    spend_by_source = (
        marketing.groupby("lead_source_standardized", dropna=False)
        .agg(marketing_spend=("spend", "sum"))
        .reset_index()
    )

    source_perf = leads_by_source.merge(retained_by_source, on="lead_source_standardized", how="left")
    source_perf = source_perf.merge(spend_by_source, on="lead_source_standardized", how="left")
    source_perf = source_perf.merge(revenue_by_source, on="lead_source_standardized", how="left")
    source_perf = source_perf.fillna(0)

    source_perf["retention_rate"] = source_perf.apply(
        lambda row: round(null_safe_divide(row["retained_cases"], row["leads"]), 4),
        axis=1,
    )
    source_perf["cost_per_retained_case"] = source_perf.apply(
        lambda row: round(null_safe_divide(row["marketing_spend"], row["retained_cases"]), 2),
        axis=1,
    )

    return source_perf.sort_values(["retained_cases", "revenue"], ascending=False)


def build_practice_area_performance(
    case_base: pd.DataFrame,
    payments: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build practice area summary.

    Returns
    -------
    pd.DataFrame
    """
    practice = case_base.copy()

    practice_perf = (
        practice.merge(payments, on="matter_id", how="left")
        .groupby("practice_area_matter", dropna=False)
        .agg(
            matters=("matter_id", "nunique"),
            retained_cases=("retained_flag", "sum"),
            revenue=("amount", "sum"),
            avg_days_to_consult=("days_to_consult", "mean"),
        )
        .reset_index()
        .rename(columns={"practice_area_matter": "practice_area"})
    )

    return practice_perf.sort_values(["revenue", "retained_cases"], ascending=False)


def build_team_workload_summary(
    leads: pd.DataFrame,
    matters: pd.DataFrame,
    communications: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build a team-level workload view.

    Returns
    -------
    pd.DataFrame
    """
    lead_assignments = (
        leads.groupby("assigned_to", dropna=False)
        .agg(leads_assigned=("lead_id", "nunique"))
        .reset_index()
        .rename(columns={"assigned_to": "team_member"})
    )

    matter_assignments = (
        matters.groupby("attorney", dropna=False)
        .agg(matters_opened=("matter_id", "nunique"))
        .reset_index()
        .rename(columns={"attorney": "team_member"})
    )

    comm_assignments = (
        communications.groupby("handled_by", dropna=False)
        .agg(communications_handled=("communication_id", "nunique"))
        .reset_index()
        .rename(columns={"handled_by": "team_member"})
    )

    team = lead_assignments.merge(matter_assignments, on="team_member", how="outer")
    team = team.merge(comm_assignments, on="team_member", how="outer")
    team = team.fillna(0)

    return team.sort_values(
        ["communications_handled", "matters_opened", "leads_assigned"],
        ascending=False,
    )


def build_communication_summary(communications: pd.DataFrame) -> pd.DataFrame:
    """
    Build channel-level communication summary.

    Returns
    -------
    pd.DataFrame
    """
    comms = communications.copy()
    comms["month"] = pd.to_datetime(comms["event_time"], errors="coerce").dt.to_period("M").astype(str)

    summary = (
        comms.groupby(["month", "channel"], dropna=False)
        .agg(total_communications=("communication_id", "nunique"))
        .reset_index()
    )

    return summary.sort_values(["month", "channel"])


def build_gold_layer() -> Dict[str, pd.DataFrame]:
    """
    Build all Gold reporting outputs from Silver tables.

    Returns
    -------
    dict[str, pd.DataFrame]
    """
    silver = load_silver_tables()

    leads = silver["leads_clean"]
    matters = silver["matters_clean"]
    communications = silver["communications_clean"]
    marketing = silver["marketing_spend_clean"]
    payments = silver["payments_clean"]

    case_base = build_case_base(leads, matters)

    gold_tables = {
        "case_base": case_base,
        "executive_monthly_kpis": build_executive_monthly_kpis(leads, case_base, marketing, payments),
        "lead_source_performance": build_lead_source_performance(leads, case_base, marketing, payments),
        "practice_area_performance": build_practice_area_performance(case_base, payments),
        "team_workload_summary": build_team_workload_summary(leads, matters, communications),
        "communication_summary": build_communication_summary(communications),
    }

    for table_name, df in gold_tables.items():
        write_csv(df, GOLD_DIR / f"{table_name}.csv")

    logger.info("Gold layer complete.")
    return gold_tables


if __name__ == "__main__":
    build_gold_layer()