"""Paths and settings shared by every step of the pipeline.

Everything lives under one root folder (default: ./data) so a run is easy to inspect and easy to
throw away. Override with the OMOP_DATA_DIR environment variable.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Paths:
    root: Path

    @property
    def raw(self) -> Path:  # what each health system "sends" us
        return self.root / "raw"

    @property
    def landing(self) -> Path:  # local copy of the landing zone (S3 or a folder)
        return self.root / "landing"

    @property
    def vocab(self) -> Path:  # vocabulary CSVs (demo or Athena)
        return self.root / "vocab"

    @property
    def omop(self) -> Path:  # Parquet output of the Spark ETL, one folder per CDM table
        return self.root / "omop"

    @property
    def warehouse(self) -> Path:  # DuckDB database the checks, study and dashboard query
        return self.root / "warehouse.duckdb"

    @property
    def reports(self) -> Path:
        return self.root / "reports"

    @property
    def site(self) -> Path:
        return self.root / "site"


def get_paths(root: str | Path | None = None) -> Paths:
    root = Path(root or os.environ.get("OMOP_DATA_DIR") or PROJECT_ROOT / "data")
    return Paths(root.resolve())


# The two fictional sources. care_site/location ids are derived from these.
SITES = {
    "CVH": {"name": "Cedar Valley Health", "format": "Epic Clarity extract (pipe-delimited)", "state": "OH"},
    "RCH": {"name": "Riverside Community Health", "format": "FHIR R4 bulk export (NDJSON)", "state": "OH"},
}

# Data window simulated for both sites.
STUDY_START = "2022-01-01"
STUDY_END = "2025-12-31"
