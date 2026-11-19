"""Create a local Spark session.

Spark runs here in "local mode" (driver and executors in one process, one thread per CPU core). The
exact same DataFrame code runs unchanged on a cluster (AWS EMR, Databricks, GCP Dataproc): only the
master URL and the storage paths change.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

_HOMEBREW_JAVA = Path("/opt/homebrew/opt/openjdk@17")


def get_spark(app: str = "omop-etl", cores: str = "*") -> SparkSession:
    if "JAVA_HOME" not in os.environ and _HOMEBREW_JAVA.exists():
        os.environ["JAVA_HOME"] = str(_HOMEBREW_JAVA)
    # Spark's Python workers must run the same Python as the driver. Without this they use whatever
    # `python3` is first on PATH (found when Airflow ran the pipeline: 3.15 workers vs a 3.12 driver).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    return (
        SparkSession.builder.master(f"local[{cores}]")
        .appName(app)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.ansi.enabled", "false")  # malformed source values become NULL, not errors
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .getOrCreate()
    )
