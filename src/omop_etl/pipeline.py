"""The pipeline as a small task graph: one definition used by the CLI and by Airflow.

Each task is a plain Python function that takes the data paths and returns a small JSON-able summary.
`TASKS` lists them with their upstream dependencies. `run_all` executes them in dependency order on a
laptop; dags/omop_pipeline.py turns the same list into an Airflow DAG, so the two can never drift apart.

    generate_sources --> land_cvh --> stage_cvh --.
                     \\-> land_rch --> stage_rch --+--> build_cdm --> load_warehouse --> data_quality
    build_vocabulary --------------------------------'                              |--> suggest_mappings
                                                                                     '--> run_study --> build_site
"""

from __future__ import annotations

import json
import os
import shutil
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from .config import PROJECT_ROOT, Paths, get_paths

DEFAULT_REVIEW_CSV = PROJECT_ROOT / "mappings" / "source_to_concept_map_review.csv"


def review_csv() -> Path:
    """The reviewed mapping file (override with OMOP_REVIEW_CSV, e.g. in tests)."""
    return Path(os.environ.get("OMOP_REVIEW_CSV") or DEFAULT_REVIEW_CSV)


@dataclass(frozen=True)
class Task:
    name: str
    fn: Callable[[Paths], dict]
    upstream: tuple[str, ...] = ()
    doc: str = ""


class FatalQualityError(RuntimeError):
    """Raised when a fatal data-quality check fails; the pipeline stops before analysis."""


# ---------------------------------------------------------------------------------------------
def generate_sources(paths: Paths) -> dict:
    from .sources import clarity, fhir

    small = os.environ.get("OMOP_SMALL") == "1"  # fast mode for CI smoke runs
    cvh = clarity.generate(paths.raw / "CVH", **({"n_patients": 600} if small else {}))
    rch = fhir.generate(paths.raw / "RCH", **({"n_patients": 500} if small else {}))
    return {"CVH_files": len(cvh["files"]), "RCH_files": len(rch["files"])}


def _land(site: str) -> Callable[[Paths], dict]:
    def fn(paths: Paths) -> dict:
        from .storage import land

        m = land(paths.raw / site, os.environ.get("OMOP_LANDING_URI"), paths.landing / site)
        return {"site": site, "files": len(m["files"]), "rows": sum(f["rows"] for f in m["files"])}

    fn.__name__ = f"land_{site.lower()}"
    return fn


def build_vocabulary(paths: Paths) -> dict:
    """Use a real Athena download if OMOP_ATHENA_DIR is set, otherwise write the demo vocabulary."""
    from .vocab.demo import write_demo_vocabulary

    athena = os.environ.get("OMOP_ATHENA_DIR")
    if athena:
        paths.vocab.mkdir(parents=True, exist_ok=True)
        for t in ("CONCEPT", "CONCEPT_RELATIONSHIP", "CONCEPT_ANCESTOR", "VOCABULARY", "DOMAIN"):
            shutil.copy2(Path(athena) / f"{t}.csv", paths.vocab / f"{t}.csv")
        return {"source": "athena", "dir": athena}
    return {"source": "demo", **write_demo_vocabulary(paths.vocab)}


def _stage(site: str) -> Callable[[Paths], dict]:
    def fn(paths: Paths) -> dict:
        from .etl.spark import get_spark
        from .etl.staging import stage_clarity, stage_fhir, write_staging

        stage = stage_clarity if site == "CVH" else stage_fhir
        return write_staging(stage(get_spark(), paths.landing / site, site), paths.root / "staging" / site)

    fn.__name__ = f"stage_{site.lower()}"
    return fn


def build_cdm(paths: Paths) -> dict:
    from .etl.cdm import build_cdm as _build
    from .etl.spark import get_spark

    return _build(get_spark(), [paths.root / "staging" / "CVH", paths.root / "staging" / "RCH"], paths.vocab,
                  review_csv(), paths.omop)


def load_warehouse(paths: Paths) -> dict:
    from .warehouse import load_warehouse as _load

    return _load(paths.warehouse, paths.omop, paths.vocab, review_csv())


def data_quality(paths: Paths) -> dict:
    from .dq.checks import run_checks, summarise

    summary = summarise(run_checks(paths.warehouse, paths.reports / "dq_results.json"))
    if summary["fatal_failures"]:
        raise FatalQualityError(f"{summary['fatal_failures']} fatal data-quality failures: {summary['failed']}")
    return summary


def suggest_mappings(paths: Paths) -> dict:
    from .mapping_assist.suggest import suggest, to_json

    new = suggest(paths.warehouse, review_csv())
    (paths.reports).mkdir(parents=True, exist_ok=True)
    (paths.reports / "mapping_suggestions_latest.json").write_text(json.dumps(to_json(new), indent=1, default=str))
    return {"new_suggestions": len(new)}


def run_study(paths: Paths) -> dict:
    import duckdb

    from .study.analysis import oracle_check, run_analysis
    from .study.cohort import build_cohort
    from .study.report import write_report

    con = duckdb.connect(str(paths.warehouse))
    cohort, attrition = build_cohort(con)
    con.close()
    res = run_analysis(cohort)
    res["attrition"] = attrition.to_dict("records")
    res["oracle"] = oracle_check(cohort, paths.raw / "_ground_truth")
    paths.reports.mkdir(parents=True, exist_ok=True)
    (paths.reports / "study_results.json").write_text(json.dumps(res, indent=1, default=float))
    write_report(res, paths.reports / "study_report.md")
    return {"n": res["n"], "events": res["events"], "hr_adjusted": res["cox_adjusted"]["hr"],
            "hr_iptw": res["cox_iptw"]["hr"]}


def build_site(paths: Paths) -> dict:
    from .site.build import build_site as _build

    out = _build(paths)
    return {"site": str(out)}


TASKS: list[Task] = [
    Task("generate_sources", generate_sources, (), "Simulate both fictional health systems' exports"),
    Task("build_vocabulary", build_vocabulary, (), "Write the demo vocabulary or copy an Athena download"),
    Task("land_cvh", _land("CVH"), ("generate_sources",), "Deliver Cedar Valley's Clarity drop; verify manifest"),
    Task("land_rch", _land("RCH"), ("generate_sources",), "Deliver Riverside's FHIR export; verify manifest"),
    Task("stage_cvh", _stage("CVH"), ("land_cvh",), "Spark: Clarity -> staging"),
    Task("stage_rch", _stage("RCH"), ("land_rch",), "Spark: FHIR -> staging"),
    Task("build_cdm", build_cdm, ("stage_cvh", "stage_rch", "build_vocabulary"), "Spark: staging -> OMOP CDM"),
    Task("load_warehouse", load_warehouse, ("build_cdm",), "DuckDB: load CDM + vocab, build eras"),
    Task("data_quality", data_quality, ("load_warehouse",), "Run 163 OHDSI-style checks; stop on fatal"),
    Task("suggest_mappings", suggest_mappings, ("data_quality",), "Suggest mappings for unmapped local codes"),
    Task("run_study", run_study, ("data_quality",), "New-user cohort, Cox/KM/IPTW, risk model"),
    Task("build_site", build_site, ("run_study", "suggest_mappings"), "Static dashboard for GitHub Pages"),
]


def ordered(tasks: list[Task] = TASKS) -> list[Task]:
    """Topological order (Kahn's algorithm); raises on a cycle."""
    remaining = {t.name: set(t.upstream) for t in tasks}
    by_name = {t.name: t for t in tasks}
    out: list[Task] = []
    while remaining:
        ready = sorted(n for n, deps in remaining.items() if not deps)
        if not ready:
            raise ValueError(f"cycle in task graph: {remaining}")
        for n in ready:
            out.append(by_name[n])
            del remaining[n]
            for deps in remaining.values():
                deps.discard(n)
    return out


def run_all(root: str | Path | None = None, only: list[str] | None = None, log=print) -> dict:
    paths = get_paths(root)
    paths.root.mkdir(parents=True, exist_ok=True)
    results = {}
    for task in ordered():
        if only and task.name not in only:
            continue
        t0 = time.time()
        log(f"> {task.name:18s} {task.doc}")
        results[task.name] = task.fn(paths)
        log(f"  done in {time.time() - t0:5.1f}s  {json.dumps(results[task.name], default=str)[:160]}")
    (paths.root / "run_summary.json").write_text(json.dumps(results, indent=1, default=str))
    return results
