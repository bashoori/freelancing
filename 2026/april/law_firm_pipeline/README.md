# Unified Law Firm Reporting System

A medallion-style data pipeline project that centralizes fragmented law firm data into trusted, dashboard-ready reporting tables.

This project simulates a real reporting environment for law firm operations, where data often lives across disconnected systems such as intake logs, case management, marketing platforms, payments, phone activity, SMS, and email. The goal is to move that data through a clear Bronze, Silver, Gold structure so leadership can rely on consistent metrics and unified dashboards.

## Why this project exists

Law firms often ask simple business questions, but the data behind those questions is rarely simple.

Examples:

- How many leads are we getting each month?
- Which marketing sources bring the best retained cases?
- How long does it take to move a lead to consultation or an open matter?
- Which practice areas generate the most revenue?
- Which team members handle the most communication volume?
- Are calls, emails, and SMS activity increasing or slowing down?

These questions require more than a dashboard. They require a reporting system underneath the dashboard that cleans, standardizes, validates, and models the data first.

That is what this project is designed to show.

## Project goal

Build a small but realistic law firm analytics platform that:

- ingests raw source files from multiple business areas
- preserves source data in a Bronze layer
- cleans and standardizes that data in a Silver layer
- models business-facing KPI tables in a Gold layer
- produces outputs ready for Power BI, Looker Studio, Streamlit, or another reporting tool

## Architecture

This project follows the medallion method.

### Bronze
Raw landing zone.

Purpose:
- preserve source data as received
- standardize column names
- add ingestion metadata
- keep traceability back to the original files

Source tables:
- leads
- matters
- calls
- sms
- emails
- marketing_spend
- payments

### Silver
Cleaned and standardized layer.

Purpose:
- parse dates
- remove duplicates
- standardize lead sources
- standardize communication channels
- handle missing values
- create a unified communications table across calls, SMS, and emails

Silver tables:
- leads_clean
- matters_clean
- communications_clean
- marketing_spend_clean
- payments_clean

### Gold
Business-ready reporting layer.

Purpose:
- join related datasets
- create KPI summaries
- support executive and operational reporting
- provide stable inputs for dashboards

Gold tables:
- case_base
- executive_monthly_kpis
- lead_source_performance
- practice_area_performance
- team_workload_summary
- communication_summary
- quality_report

## Project structure

```text
law_firm_pipeline/
├── data/
│   ├── source/
│   │   ├── leads.csv
│   │   ├── matters.csv
│   │   ├── calls.csv
│   │   ├── sms.csv
│   │   ├── emails.csv
│   │   ├── marketing_spend.csv
│   │   └── payments.csv
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── docs/
├── img/
├── sql/
├── src/
│   ├── bronze.py
│   ├── silver.py
│   ├── gold.py
│   ├── quality.py
│   ├── utils.py
│   └── pipeline.py
├── requirements.txt
└── README.md
```

## Business questions this project answers

This project is organized around reporting questions a law firm operations leader, managing partner, or practice manager might ask.

### Intake and conversion
- How many leads are created per month?
- How many leads become consultations?
- How many consultations become retained matters?
- What is the retention rate?

### Marketing performance
- Which lead sources generate the most leads?
- Which sources generate the most retained cases?
- How much revenue comes from each source?
- What is the cost per retained case by source?

### Operations and case flow
- How long does it take to move from lead creation to consultation?
- How long does it take to move from lead creation to matter opening?
- Which practice areas are most active?
- Which practice areas generate the most revenue?

### Communications and team activity
- How many calls, texts, and emails are being handled?
- Which channels are most active?
- Which team members are carrying the most workload?
- Is communication volume trending up or down over time?

## Source data

This project uses sample CSV files to simulate common law firm reporting inputs.

### `leads.csv`
Tracks intake activity and lead origin.

Typical fields:
- `lead_id`
- `created_at`
- `lead_source`
- `practice_area`
- `assigned_to`
- `status`

### `matters.csv`
Tracks cases or matters opened from leads.

Typical fields:
- `matter_id`
- `lead_id`
- `opened_at`
- `consultation_at`
- `retained_flag`
- `practice_area`
- `attorney`
- `status`

### `calls.csv`
Tracks phone activity.

Typical fields:
- `call_id`
- `lead_id`
- `matter_id`
- `call_time`
- `direction`
- `handled_by`

### `sms.csv`
Tracks text message activity.

Typical fields:
- `sms_id`
- `lead_id`
- `matter_id`
- `sent_at`
- `direction`
- `handled_by`

### `emails.csv`
Tracks email communication.

Typical fields:
- `email_id`
- `lead_id`
- `matter_id`
- `sent_at`
- `direction`
- `handled_by`

### `marketing_spend.csv`
Tracks spend by source and month.

Typical fields:
- `month`
- `lead_source`
- `spend`

### `payments.csv`
Tracks case revenue.

Typical fields:
- `payment_id`
- `matter_id`
- `payment_date`
- `amount`

## Pipeline flow

The full pipeline runs in this order:

1. Read source CSV files from `data/source`
2. Build Bronze tables in `data/bronze`
3. Build Silver tables in `data/silver`
4. Run data quality checks on Silver outputs
5. Build Gold reporting tables in `data/gold`
6. Save a quality report for trust and validation

## How to run the project

### 1. Clone the repository

```bash
git clone https://github.com/bashoori/freelancing.git
cd freelancing/2026/april/law_firm_pipeline
```

### 2. Create and activate a virtual environment

#### macOS / Linux
```bash
python -m venv .venv
source .venv/bin/activate
```

#### Windows
```bash
python -m venv .venv
.venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Add source files

Place the required CSV files in:

```text
data/source/
```

Expected files:

- `leads.csv`
- `matters.csv`
- `calls.csv`
- `sms.csv`
- `emails.csv`
- `marketing_spend.csv`
- `payments.csv`

### 5. Run the pipeline

```bash
python src/pipeline.py
```

## Outputs

After a successful run, the pipeline writes files to the following locations.

### Bronze outputs
- `data/bronze/leads.csv`
- `data/bronze/matters.csv`
- `data/bronze/calls.csv`
- `data/bronze/sms.csv`
- `data/bronze/emails.csv`
- `data/bronze/marketing_spend.csv`
- `data/bronze/payments.csv`

### Silver outputs
- `data/silver/leads_clean.csv`
- `data/silver/matters_clean.csv`
- `data/silver/communications_clean.csv`
- `data/silver/marketing_spend_clean.csv`
- `data/silver/payments_clean.csv`

### Gold outputs
- `data/gold/case_base.csv`
- `data/gold/executive_monthly_kpis.csv`
- `data/gold/lead_source_performance.csv`
- `data/gold/practice_area_performance.csv`
- `data/gold/team_workload_summary.csv`
- `data/gold/communication_summary.csv`
- `data/gold/quality_report.csv`

## What each Gold table means

### `case_base.csv`
A case-centric analytical table that combines lead and matter data.

Used for:
- lead-to-matter analysis
- time-to-consult analysis
- source-to-revenue linking
- practice area reporting

### `executive_monthly_kpis.csv`
A monthly KPI summary for leadership reporting.

Includes:
- leads
- consultations
- retained cases
- revenue
- marketing spend
- retention rate
- cost per retained case

### `lead_source_performance.csv`
A source-level performance view.

Includes:
- total leads
- retained cases
- retention rate
- total spend
- revenue
- cost per retained case

### `practice_area_performance.csv`
A practice area summary.

Includes:
- matter volume
- retained cases
- revenue
- average days to consult

### `team_workload_summary.csv`
A workload summary by team member.

Includes:
- leads assigned
- matters opened
- communications handled

### `communication_summary.csv`
A monthly and channel-level communication view.

Includes:
- calls
- SMS
- emails
- communication activity trends

### `quality_report.csv`
A data quality summary from the Silver layer.

Includes checks such as:
- required columns present
- null values in key fields
- duplicate keys
- negative values
- invalid date ordering

## Data quality approach

Reliable dashboards depend on reliable inputs.

This project includes a quality layer to make that visible.

Checks include:
- required columns exist
- key IDs are not missing
- duplicates are flagged
- negative spend or payment values are flagged
- invalid date order is flagged

This matters because reporting systems fail when trust in the data breaks down. A metric is only useful if the team believes it.

## Design decisions

### Why medallion
The medallion structure makes the pipeline easier to reason about.

- Bronze preserves source truth
- Silver creates consistency
- Gold creates business meaning

This separation makes the project easier to debug, explain, and extend.

### Why communications are unified in Silver
Calls, SMS, and emails often come from different systems and have different schemas. By standardizing them into one `communications_clean` table, the reporting layer can analyze activity in a consistent way across channels.

### Why Gold tables are pre-aggregated
Dashboards should consume stable reporting outputs, not re-implement business logic inside visualizations. That keeps metrics consistent across tools and reduces complexity in the reporting layer.

## Example use cases

This project can support reporting scenarios such as:

- executive leadership dashboard
- intake and conversion dashboard
- marketing ROI review
- practice area revenue tracking
- team workload monitoring
- communication activity reporting

## Dashboard ideas

A natural next step is connecting the Gold outputs to a reporting tool.

Suggested dashboard pages:

### Executive Overview
- leads
- retained cases
- retention rate
- revenue
- marketing spend
- cost per retained case
- monthly trend charts

### Intake and Communications
- consultations
- avg days to consult
- calls vs SMS vs email
- monthly communication trends
- team handling volume

### Source and Practice Performance
- retained cases by source
- revenue by source
- cost per retained case by source
- revenue by practice area
- conversion performance by area

## Example screenshots

Add screenshots here once the dashboard is built.

Example section:

```md
## Dashboard Preview

### Executive Overview
![Executive Overview](img/executive_overview.png)

### Intake and Communications
![Intake Dashboard](img/intake_dashboard.png)

### Source Performance
![Source Performance](img/source_performance.png)
```

## Limitations of the current version

This is a local CSV-based implementation meant to demonstrate architecture, modeling, and reporting logic.

Current limitations:
- local file inputs rather than live APIs
- no orchestration scheduler yet
- no database backend yet
- no automated tests yet
- no dashboard app included by default in this README
- sample data may be simpler than real production law firm systems

## Future improvements

Strong next steps for this project would be:

- generate more realistic messy sample data
- add unit tests for transformations and quality checks
- load Bronze data into a database instead of CSV
- use dbt for Silver and Gold transformations
- orchestrate pipeline runs with Airflow
- deploy a dashboard with Streamlit or Power BI
- support API ingestion from CRM or communication systems
- add response-time metrics with more detailed event logic
- add matter stage funnel reporting
- add client-level profitability analysis

## Why this project is useful in a portfolio

This project shows more than dashboard building.

It demonstrates:
- data engineering structure
- ETL thinking
- medallion modeling
- data quality discipline
- business-oriented reporting design
- the ability to translate fragmented operational data into decision-ready outputs

That is the real value of the project.

## Author

**Bita Ashoori**  
Data Engineer  
GitHub: [https://github.com/bashoori](https://github.com/bashoori)

## License

This project is for portfolio and learning purposes.
