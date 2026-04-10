
# Law Firm Reporting Pipeline Portfolio Project

A GitHub-ready portfolio project that simulates the kind of work described in a real Upwork role: centralizing fragmented law-firm data from operations, marketing, finance, and communications into a reporting-ready model.

## Business problem

Law firms often operate across disconnected systems:
- case management
- marketing platforms
- finance tools
- phone, SMS, and email systems

Leadership needs one place to track performance. This project shows how to ingest that data, standardize it, and produce KPI-ready outputs for dashboards.

## What this project demonstrates

- Multi-source data ingestion using Python
- Basic silver/gold modeling for reporting
- KPI generation for executive dashboards
- A realistic repo structure you can present in GitHub
- Business framing that matches fixed-price analytics and data engineering work

## Tech stack

- Python
- pandas
- SQL
- CSV sample data
- Power BI or Looker Studio for dashboarding

## Repository structure

```text
law_firm_reporting_portfolio/
├── data/
│   ├── raw/
│   └── processed/
├── docs/
│   └── dashboard_spec.md
├── sql/
│   └── exec_monthly_kpis.sql
├── src/
│   └── pipeline.py
├── requirements.txt
└── README.md
```

## Sample datasets

This repo includes synthetic sample data for:
- `matters.csv`
- `leads.csv`
- `communications.csv`
- `marketing_spend.csv`
- `team_performance.csv`

These are fictional and designed only for portfolio use.

## KPI outputs produced

The pipeline writes these processed outputs:
- `silver_matters.csv`
- `gold_exec_monthly_kpis.csv`
- `gold_marketing_monthly.csv`
- `gold_source_practice_kpis.csv`
- `gold_team_performance.csv`

## Example business questions answered

- Which lead sources drive the highest retention rate?
- What is the cost per retained case over time?
- How quickly are new matters receiving consultation?
- Which practice areas generate the most fee revenue?
- How does communication responsiveness relate to outcomes?

## How to run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python src/pipeline.py
```

## Suggested next improvements

- Load raw data into DuckDB or BigQuery
- Add dbt models for transformations
- Add tests for nulls, duplicates, and referential integrity
- Build a Power BI dashboard from the gold tables
- Add orchestration with Airflow or GitHub Actions
- Add a client-level dimension to simulate a multi-firm portfolio

## How to present this on Upwork

Position it as:
> A reusable reporting foundation for fragmented professional-services data, where operational, marketing, financial, and communication data are unified into clean KPI models and executive dashboards.

That framing is stronger than saying you built a few dashboards.

## Architecture

![Architecture](docs/architecture.png)

## Dashboard Mockup

![Dashboard Mockup](docs/dashboard_mockup.png)
