from __future__ import annotations

from pathlib import Path
import pandas as pd
import streamlit as st

BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data" / "processed"

st.set_page_config(page_title="Law Firm Unified Dashboard", layout="wide")
st.title("Law Firm Unified Reporting Demo")
st.caption("Portfolio project showing ETL, centralized data marts, and leadership reporting for law firm operations.")

kpis = pd.read_csv(PROCESSED_DIR / "kpis.csv")
lead_funnel = pd.read_csv(PROCESSED_DIR / "mart_lead_funnel.csv", parse_dates=["created_at"])
case_perf = pd.read_csv(PROCESSED_DIR / "mart_case_performance.csv", parse_dates=["opened_at", "closed_at"])
monthly_messages = pd.read_csv(PROCESSED_DIR / "monthly_communications.csv")

metric_map = dict(zip(kpis["metric"], kpis["value"]))
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Leads", int(metric_map["total_leads"]))
col2.metric("Signed Leads", int(metric_map["signed_leads"]))
col3.metric("Lead to Signed Rate", f"{metric_map['lead_to_signed_rate']:.1%}")
col4.metric("Revenue Received", f"${metric_map['total_revenue']:,.0f}")

left, right = st.columns(2)
with left:
    st.subheader("Leads by Marketing Source")
    leads_by_source = lead_funnel.groupby("marketing_source", as_index=False).size().rename(columns={"size": "lead_count"})
    st.bar_chart(leads_by_source.set_index("marketing_source"))

with right:
    st.subheader("Revenue by Case Type")
    revenue_by_case = case_perf.groupby("case_type", as_index=False)["payment_received"].sum()
    st.bar_chart(revenue_by_case.set_index("case_type"))

st.subheader("Communication Trend")
st.line_chart(monthly_messages.set_index("month"))

st.subheader("Case Performance")
st.dataframe(
    case_perf[["case_id", "case_type", "case_status", "primary_attorney", "payment_received", "days_to_close"]]
    .sort_values(by="payment_received", ascending=False)
    .reset_index(drop=True),
    use_container_width=True,
)
