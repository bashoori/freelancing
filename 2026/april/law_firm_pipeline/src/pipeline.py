from __future__ import annotations

"""
Main pipeline runner for the law firm medallion project.

Execution order
---------------
1. Build Bronze
2. Build Silver
3. Run Silver quality checks
4. Build Gold
5. Save quality report to Gold layer

Why this file should stay short:
- pipeline.py should read like a story
- each layer should keep its own logic
- easier to debug and maintain
"""

from bronze import build_bronze_layer
from silver import build_silver_layer
from gold import build_gold_layer
from quality import run_silver_quality_checks
from utils import GOLD_DIR, write_csv, ensure_directories, logger


def run_pipeline() -> None:
    """
    Run the full medallion pipeline from source to Gold.
    """
    ensure_directories()

    logger.info("Starting Bronze layer...")
    build_bronze_layer()

    logger.info("Starting Silver layer...")
    silver_tables = build_silver_layer()

    logger.info("Running quality checks...")
    quality_report = run_silver_quality_checks(silver_tables)
    write_csv(quality_report, GOLD_DIR / "quality_report.csv")

    logger.info("Starting Gold layer...")
    build_gold_layer()

    logger.info("Pipeline complete.")


if __name__ == "__main__":
    run_pipeline()