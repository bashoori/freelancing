# Job Data Ingestion & Simple BI Reporting 

This project builds a simple data pipeline that ingests job postings from an external API, cleans and validates the data, and stores it in a structured Postgres database for analysis and reporting.

The goal is not just to collect data, but to make it reliable, queryable, and ready for downstream use.

---

## What this project does

- Pulls job postings from an external API (RemoteOK)
- Cleans and normalizes inconsistent data fields
- Validates required fields (title, URL, ID)
- Prevents duplicate records using a unique constraint
- Stores data in Postgres with a clear schema
- Logs pipeline runs (success/failure, record counts)
- Provides a reporting-ready view (`recent_jobs`)

---

## Architecture

The pipeline follows a simple and reliable ETL flow:

- **Extract**  
  Fetch job data from an API

- **Transform**  
  Normalize fields and validate records

- **Load**  
  Insert clean data into Postgres with deduplication

- **Log**  
  Track ingestion runs for monitoring and debugging

This mirrors how production data pipelines are designed, just at a smaller scale.

---

## Project Structure
job-data-pipeline/
│
├── data/
├── logs/
├── src/
│ └── pipeline.py
│
├── db/
│ └── schema.sql
│
├── requirements.txt
├── .env.example
└── README.md


---

## Database Design

The database includes three main tables:

- **sources**  
  Tracks where data comes from

- **jobs**  
  Stores cleaned job postings  
  Uses `(source_id, source_job_id)` to prevent duplicates

- **ingestion_runs**  
  Logs each pipeline execution (status, records loaded, errors)

---

## How to run it

python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

docker run --name job-postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=jobs \
  -p 5433:5432 \
  -d postgres:16

docker exec -i job-postgres psql -U postgres -d jobs < db/schema.sql

python src/pipeline.py





Example query:
SELECT title, company, location, posted_at
FROM recent_jobs
ORDER BY posted_at DESC;