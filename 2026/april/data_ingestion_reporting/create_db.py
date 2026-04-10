from __future__ import annotations

import os

import psycopg2
from dotenv import load_dotenv


load_dotenv()


def main() -> None:
    conn = psycopg2.connect(
        dbname="postgres",
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres"),
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("SELECT 1 FROM pg_database WHERE datname = 'jobs';")
    exists = cur.fetchone()

    if exists:
        print("Database 'jobs' already exists.")
    else:
        cur.execute("CREATE DATABASE jobs;")
        print("Database 'jobs' created.")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()