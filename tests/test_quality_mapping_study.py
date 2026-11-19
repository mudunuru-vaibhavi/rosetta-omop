import math
import shutil

import duckdb
import pytest

from omop_etl.dq.checks import run_checks, summarise
from omop_etl.mapping_assist.suggest import Candidate, Suggestion, suggest, validate
from omop_etl.pipeline import TASKS, ordered


# --------------------------------------------------------------------------- data quality
def test_quality_checks_pass_and_find_the_planted_defects(pipeline):
    results = run_checks(pipeline["paths"].warehouse)
    s = summarise(results)
    assert s["fatal_failures"] == 0
    flagged = {(r.check_name, r.table) for r in results if r.flagged}
    assert ("plausibleValueHigh", "person") in flagged, "birth year typed 100 years late"
    assert ("plausibleStartBeforeEnd", "visit_occurrence") in flagged, "encounters ending before they start"


def test_a_duplicate_key_is_fatal(pipeline, tmp_path):
    db = tmp_path / "broken.duckdb"
    shutil.copy(pipeline["paths"].warehouse, db)
    c = duckdb.connect(str(db))
    c.execute("INSERT INTO cdm.person SELECT * FROM cdm.person LIMIT 1")
    c.close()
    s = summarise(run_checks(db))
    assert s["fatal_failures"] >= 1


# --------------------------------------------------------------------------- assisted mapping
def _s(kind="lab", desc="POC HEMOGLOBIN A1C", unit="%"):
    return Suggestion("CVH", "CVH_LAB_COMPONENT", "1511", desc, kind, 100, unit)


def test_validator_rejects_wrong_domain_and_release_form(con):
    lab = _s()
    cid = con.execute("SELECT concept_id FROM cdm.concept WHERE concept_code = '310490'").fetchone()[0]
    lab.chosen = Candidate(cid, "RxNorm", "310490", "glipizide 5 MG Oral Tablet", "Clinical Drug", 0.5)
    assert any(v.startswith("FAIL: domain") for v in validate(con, lab))

    drug = _s("drug", "Metformin ER 500mg tab", None)
    cid = con.execute("SELECT concept_id FROM cdm.concept WHERE concept_code = '861007'").fetchone()[0]
    drug.chosen = Candidate(cid, "RxNorm", "861007", "metformin hydrochloride 500 MG Oral Tablet", "Clinical Drug", 0.5)
    assert any("release form" in v for v in validate(con, drug))

    ghost = _s()
    ghost.chosen = Candidate(123, "LOINC", "0000-0", "invented", "Lab Test", 0.9)
    assert validate(con, ghost) == ["FAIL: concept does not exist in the vocabulary"]


def test_suggest_never_overwrites_reviewed_rows(pipeline):
    before = pipeline["review"].read_text()
    assert suggest(pipeline["paths"].warehouse, pipeline["review"]) == []
    assert pipeline["review"].read_text() == before


# --------------------------------------------------------------------------- study
def test_cohort_attrition_never_grows(con):
    counts = [r[1] for r in con.execute("SELECT * FROM results.study_attrition").fetchall()]
    assert counts == sorted(counts, reverse=True) and counts[-1] > 50


def test_study_produces_finite_estimates(pipeline):
    import json

    r = json.loads((pipeline["paths"].reports / "study_results.json").read_text())
    for k in ("cox_unadjusted", "cox_adjusted", "cox_iptw"):
        assert math.isfinite(r[k]["hr"]) and r[k]["lo"] < r[k]["hr"] < r[k]["hi"]
    assert r["oracle"]["arm_agreement_pct"] > 99
    assert all(abs(x["smd_after_iptw"]) < 0.1 for x in r["table1"]), "IPTW balances the groups"
    assert (pipeline["paths"].site / "index.html").exists()


# --------------------------------------------------------------------------- orchestration
def test_task_graph_is_a_valid_dag():
    names = {t.name for t in TASKS}
    assert all(u in names for t in TASKS for u in t.upstream)
    order = [t.name for t in ordered()]
    for t in TASKS:
        assert all(order.index(u) < order.index(t.name) for u in t.upstream)


def test_airflow_dag_matches_task_list(project_root):
    pytest.importorskip("airflow")
    from airflow.models.dagbag import DagBag

    bag = DagBag(dag_folder=str(project_root / "dags"), include_examples=False)
    assert not bag.import_errors
    dag = bag.get_dag("rosetta_omop")
    assert {t.task_id for t in dag.tasks} == {t.name for t in TASKS}
