from __future__ import annotations

from pathlib import Path
import random
from datetime import datetime, timedelta
import pandas as pd

RAW_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"
RANDOM_SEED = 42
random.seed(RANDOM_SEED)

CAMPAIGNS = [
    ("Google Ads", "Paid Search"),
    ("Meta Ads", "Paid Social"),
    ("Organic Search", "SEO"),
    ("Referral", "Partner"),
    ("Direct", "Direct"),
]
CASE_TYPES = ["Personal Injury", "Family Law", "Immigration", "Employment"]
STATUSES = ["New Lead", "Qualified", "Signed", "In Progress", "Closed Won", "Closed Lost"]
STAFF = [
    (1, "Maria Lopez", "Intake Specialist"),
    (2, "James Patel", "Attorney"),
    (3, "Sarah Kim", "Paralegal"),
    (4, "Daniel Reed", "Case Manager"),
]


def create_leads(n: int = 200) -> pd.DataFrame:
    start = datetime(2026, 1, 1)
    rows = []
    for i in range(1, n + 1):
        created_at = start + timedelta(days=random.randint(0, 95), hours=random.randint(8, 18))
        source, channel = random.choice(CAMPAIGNS)
        case_type = random.choice(CASE_TYPES)
        status = random.choices(STATUSES, weights=[22, 18, 16, 16, 18, 10])[0]
        rows.append(
            {
                "lead_id": i,
                "created_at": created_at,
                "client_name": f"Client {i}",
                "case_type": case_type,
                "marketing_source": source,
                "marketing_channel": channel,
                "city": random.choice(["Jacksonville", "Orlando", "Tampa", "Miami"]),
                "status": status,
                "assigned_staff_id": random.randint(1, 4),
                "estimated_case_value": round(random.uniform(1500, 25000), 2),
            }
        )
    return pd.DataFrame(rows)


def create_cases(leads: pd.DataFrame) -> pd.DataFrame:
    signed = leads[leads["status"].isin(["Signed", "In Progress", "Closed Won"])]
    rows = []
    for _, lead in signed.iterrows():
        opened_at = pd.to_datetime(lead["created_at"]) + timedelta(days=random.randint(0, 10))
        closed_at = None
        case_status = random.choices(["Open", "Closed Won", "Closed Lost"], weights=[55, 35, 10])[0]
        if case_status != "Open":
            closed_at = opened_at + timedelta(days=random.randint(15, 70))
        rows.append(
            {
                "case_id": f"C{lead['lead_id']:04d}",
                "lead_id": lead["lead_id"],
                "opened_at": opened_at,
                "closed_at": closed_at,
                "case_status": case_status,
                "case_type": lead["case_type"],
                "primary_attorney_id": random.randint(1, 4),
                "settlement_amount": round(random.uniform(5000, 80000), 2) if case_status == "Closed Won" else 0,
            }
        )
    return pd.DataFrame(rows)


def create_communications(leads: pd.DataFrame, channel: str, count_range: tuple[int, int]) -> pd.DataFrame:
    rows = []
    for _, lead in leads.iterrows():
        for idx in range(random.randint(*count_range)):
            sent_at = pd.to_datetime(lead["created_at"]) + timedelta(days=random.randint(0, 45), hours=random.randint(8, 18))
            rows.append(
                {
                    f"{channel}_id": f"{channel[:1].upper()}{lead['lead_id']}_{idx + 1}",
                    "lead_id": lead["lead_id"],
                    "sent_at": sent_at,
                    "direction": random.choice(["Inbound", "Outbound"]),
                    "duration_seconds": random.randint(45, 1200) if channel == "calls" else None,
                    "status": random.choice(["Delivered", "Completed", "Failed"]),
                }
            )
    return pd.DataFrame(rows)


def create_financials(cases: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, case in cases.iterrows():
        month_count = random.randint(1, 3)
        base_date = pd.to_datetime(case["opened_at"])
        for i in range(month_count):
            bill_date = base_date + timedelta(days=30 * i)
            rows.append(
                {
                    "txn_id": f"T{case['case_id']}_{i + 1}",
                    "case_id": case["case_id"],
                    "bill_date": bill_date,
                    "fee_amount": round(random.uniform(500, 4000), 2),
                    "cost_amount": round(random.uniform(100, 1000), 2),
                    "payment_received": round(random.uniform(300, 3500), 2),
                }
            )
    return pd.DataFrame(rows)


def create_staff() -> pd.DataFrame:
    return pd.DataFrame(STAFF, columns=["staff_id", "staff_name", "role"])


def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    leads = create_leads()
    cases = create_cases(leads)
    calls = create_communications(leads, "calls", (1, 5))
    sms = create_communications(leads, "sms", (0, 4))
    emails = create_communications(leads, "emails", (0, 6))
    financials = create_financials(cases)
    staff = create_staff()

    leads.to_csv(RAW_DIR / "leads.csv", index=False)
    cases.to_csv(RAW_DIR / "cases.csv", index=False)
    calls.to_csv(RAW_DIR / "calls.csv", index=False)
    sms.to_csv(RAW_DIR / "sms.csv", index=False)
    emails.to_csv(RAW_DIR / "emails.csv", index=False)
    financials.to_csv(RAW_DIR / "financials.csv", index=False)
    staff.to_csv(RAW_DIR / "staff.csv", index=False)

    print(f"Sample data generated in {RAW_DIR}")


if __name__ == "__main__":
    main()
