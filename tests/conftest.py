"""Shared fixtures: one small end-to-end pipeline run (about 1,100 patients) reused by the tests."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from omop_etl.config import get_paths
from omop_etl.pipeline import DEFAULT_REVIEW_CSV, run_all


@pytest.fixture(scope="session")
def pipeline(tmp_path_factory):
    root = tmp_path_factory.mktemp("omop")
    review = root / "review.csv"
    shutil.copy(DEFAULT_REVIEW_CSV, review)  # never touch the real reviewed file
    mp = pytest.MonkeyPatch()
    mp.setenv("OMOP_SMALL", "1")
    mp.setenv("OMOP_REVIEW_CSV", str(review))
    mp.delenv("OMOP_LANDING_URI", raising=False)
    results = run_all(root, log=lambda *_: None)
    yield {"paths": get_paths(root), "results": results, "review": review}
    mp.undo()


@pytest.fixture()
def con(pipeline):
    import duckdb

    c = duckdb.connect(str(pipeline["paths"].warehouse), read_only=True)
    yield c
    c.close()


@pytest.fixture()
def project_root() -> Path:
    return Path(__file__).resolve().parents[1]
