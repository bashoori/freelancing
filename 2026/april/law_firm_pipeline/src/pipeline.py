
from pathlib import Path
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
PROCESSED = ROOT / "data" / "processed"
PROCESSED.mkdir(parents=True, exist_ok=True)

def load():
    matters = pd.read_csv(RAW / "matters.csv", parse_dates=["opened_date","consult_date","closed_date"])
    leads = pd.read_csv(RAW / "leads.csv", parse_dates=["lead_created_date"])
    comms = pd.read_csv(RAW / "communications.csv", parse_dates=["comm_date"])
    marketing = pd.read_csv(RAW / "marketing_spend.csv", parse_dates=["date"])
    team = pd.read_csv(RAW / "team_performance.csv")
    return matters, leads, comms, marketing, team

def build_silver(matters, leads, comms):
    matter_base = matters.merge(leads, on=["lead_id","lead_source","practice_area"], how="left")
    matter_base["days_to_consult"] = (matter_base["consult_date"] - matter_base["opened_date"]).dt.days
    matter_base["days_to_close"] = (matter_base["closed_date"] - matter_base["consult_date"]).dt.days
    comm_agg = (
        comms.groupby(["matter_id","channel"], as_index=False)
        .agg(message_count=("channel","size"), avg_response_minutes=("response_minutes","mean"), total_call_duration_sec=("duration_sec","sum"))
    )
    comm_wide = comm_agg.pivot(index="matter_id", columns="channel", values="message_count").fillna(0).reset_index()
    response = comms.groupby("matter_id", as_index=False).agg(
        total_interactions=("channel","size"),
        avg_response_minutes=("response_minutes","mean"),
        total_call_duration_sec=("duration_sec","sum")
    )
    silver = matter_base.merge(response, on="matter_id", how="left").merge(comm_wide, on="matter_id", how="left")
    silver.to_csv(PROCESSED / "silver_matters.csv", index=False)
    return silver

def build_gold(silver, marketing, team):
    silver["open_month"] = silver["opened_date"].dt.to_period("M").astype(str)
    kpi_month = silver.groupby("open_month", as_index=False).agg(
        leads=("lead_id","nunique"),
        retained_cases=("retained","sum"),
        closed_won=("status", lambda s: (s=="Closed Won").sum()),
        fee_revenue=("fee_revenue","sum"),
        avg_days_to_consult=("days_to_consult","mean"),
        avg_response_minutes=("avg_response_minutes","mean"),
        total_interactions=("total_interactions","sum")
    )
    kpi_month["retention_rate"] = np.where(kpi_month["leads"]>0, kpi_month["retained_cases"]/kpi_month["leads"], 0)

    marketing["month"] = marketing["date"].dt.to_period("M").astype(str)
    mkt_month = marketing.groupby(["month","campaign_source"], as_index=False).agg(
        spend=("spend","sum"), clicks=("clicks","sum"), impressions=("impressions","sum")
    )
    spend_totals = marketing.groupby("month", as_index=False).agg(marketing_spend=("spend","sum"))
    gold = kpi_month.merge(spend_totals, left_on="open_month", right_on="month", how="left").drop(columns=["month"])
    gold["cost_per_retained_case"] = np.where(gold["retained_cases"]>0, gold["marketing_spend"]/gold["retained_cases"], np.nan)
    gold["revenue_per_lead"] = np.where(gold["leads"]>0, gold["fee_revenue"]/gold["leads"], 0)

    source_kpi = silver.groupby(["lead_source","practice_area"], as_index=False).agg(
        leads=("lead_id","nunique"),
        retained_cases=("retained","sum"),
        fee_revenue=("fee_revenue","sum"),
        avg_days_to_consult=("days_to_consult","mean")
    )
    source_kpi["retention_rate"] = source_kpi["retained_cases"] / source_kpi["leads"]

    team.to_csv(PROCESSED / "gold_team_performance.csv", index=False)
    mkt_month.to_csv(PROCESSED / "gold_marketing_monthly.csv", index=False)
    source_kpi.to_csv(PROCESSED / "gold_source_practice_kpis.csv", index=False)
    gold.to_csv(PROCESSED / "gold_exec_monthly_kpis.csv", index=False)

def main():
    matters, leads, comms, marketing, team = load()
    silver = build_silver(matters, leads, comms)
    build_gold(silver, marketing, team)
    print("Pipeline complete. Files written to data/processed.")

if __name__ == "__main__":
    main()
