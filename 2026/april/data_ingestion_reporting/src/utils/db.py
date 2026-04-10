from __future__ import annotations

import os

import psycopg2
from dotenv import load_dotenv


load_dotenv()


def get_connection():
    """
    Create and return a Postgres connection using environment variables.
    """
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME", "jobs"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres"),
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
    )