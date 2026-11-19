"""Build the static dashboard (one HTML file + data embedded) for GitHub Pages."""

from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path

import duckdb

from ..config import SITES, Paths
from ..pipeline import review_csv

TEMPLATE = Path(__file__).with_name("template.html")
SITE_OF = "split_part(p.person_source_value, '|', 1)"

# Field-level lineage: how each OMOP field is filled from each source (shown on the Lineage tab).
LINEAGE = [
    ("person", "person_id", "hash(CVH, PATIENT.PAT_ID)", "hash(RCH, Patient.id)", "Deterministic 53-bit hash"),
    ("person", "gender_concept_id", "PATIENT.SEX_C via ZC_SEX", "Patient.gender", "Crosswalk to Gender vocabulary"),
    ("person", "race_concept_id", "PATIENT_RACE.PATIENT_RACE_C", "US Core race extension (CDC code)",
     "Crosswalk to Race vocabulary; unknown -> 0"),
    ("person", "year_of_birth", "PATIENT.BIRTH_DATE", "Patient.birthDate", "Year/month/day split"),
    ("visit_occurrence", "visit_concept_id", "PAT_ENC.ENC_TYPE_C + PAT_ENC_HSP.ADT_PAT_CLASS_C",
     "Encounter.class (AMB/VR/EMER/IMP)", "Crosswalk to Visit / Place of Service"),
    ("visit_occurrence", "visit_start_datetime", "HOSP_ADMSN_TIME, else CHECKIN_TIME", "Encounter.period.start",
     "As recorded"),
    ("condition_occurrence", "condition_concept_id", "PAT_ENC_DX -> EDG_CURRENT_ICD10.CODE",
     "Condition.code (SNOMED preferred, else ICD-10-CM)", "Maps to standard SNOMED; routed by domain"),
    ("measurement", "measurement_concept_id", "ORDER_RESULTS.COMPONENT_ID -> DEFAULT_LNC_CODE (or local id)",
     "Observation.code / component.code (LOINC or local)", "LOINC; local codes via SOURCE_TO_CONCEPT_MAP"),
    ("measurement", "value_as_number", "ORD_NUM_VALUE; 9999999 -> parse ORD_VALUE ('>14.0')",
     "valueQuantity.value", "Unit-harmonised (oz/lb->kg, in->cm, IFCC->NGSP %)"),
    ("measurement", "measurement_concept_id (vitals)", "IP_FLWSHT_MEAS rows 5/14/11; BP '128/82' split",
     "BP panel 85354-9 components; 29463-7; 8302-2", "LOINC 8480-6, 8462-4, 29463-7, 8302-2"),
    ("drug_exposure", "drug_concept_id", "ORDER_MED.MEDICATION_ID -> RXNORM_CODES (or local id)",
     "MedicationRequest.medicationCodeableConcept (RxNorm or text)", "RxNorm SCD; local via SOURCE_TO_CONCEPT_MAP"),
    ("drug_exposure", "days_supply", "END_DATE - START_DATE", "dispenseRequest.expectedSupplyDuration", "Days"),
    ("procedure_occurrence", "procedure_concept_id", "ORDER_PROC.PROC_ID -> CLARITY_EAP.PROC_CODE (CPT)",
     "Procedure.code (CPT)", "Lab-panel CPT codes route to MEASUREMENT"),
    ("observation", "observation_concept_id", "ICD-10-CM Z87.891 (history of tobacco use)", "SNOMED 8517006",
     "Domain of the standard concept decides the table"),
    ("death", "death_date", "PATIENT.DEATH_DATE", "Patient.deceasedDateTime", "As recorded"),
    ("observation_period", "start/end", "first / last recorded activity", "first / last recorded activity",
     "Standard EHR convention"),
]


def _rows(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict]:
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def collect(paths: Paths) -> dict:
    con = duckdb.connect(str(paths.warehouse), read_only=True)
    d: dict = {"generated": datetime.now().strftime("%Y-%m-%d %H:%M"), "sites": SITES}

    tables = ["visit_occurrence", "condition_occurrence", "drug_exposure", "measurement", "observation",
              "procedure_occurrence"]
    d["counts"] = _rows(con, "SELECT " + SITE_OF + " AS site, COUNT(*) AS person FROM cdm.person p GROUP BY 1 ORDER BY 1")
    for t in tables:
        for r in _rows(con, f"SELECT {SITE_OF} AS site, COUNT(*) AS n FROM cdm.{t} x JOIN cdm.person p "
                            f"USING (person_id) GROUP BY 1"):
            next(c for c in d["counts"] if c["site"] == r["site"])[t] = r["n"]
    d["totals"] = {t: con.execute(f"SELECT COUNT(*) FROM cdm.{t}").fetchone()[0]
                   for t in ["person", *tables, "drug_era", "condition_era", "death"]}
    d["source_rows"] = json.loads((paths.root / "run_summary.json").read_text()) if (
        paths.root / "run_summary.json").exists() else {}

    d["coverage"] = _rows(con, """
        SELECT site, event_kind, SUM(records) AS records,
               SUM(CASE WHEN mapped THEN records ELSE 0 END) AS mapped,
               SUM(CASE WHEN mapped_via = 'source_to_concept_map' THEN records ELSE 0 END) AS via_stcm,
               COUNT(*) AS codes, SUM(CASE WHEN mapped THEN 0 ELSE 1 END) AS unmapped_codes
        FROM etl.source_coverage GROUP BY ALL ORDER BY site, event_kind""")
    d["coverage_by_vocab"] = _rows(con, """
        SELECT site, vocabulary_id, COUNT(*) AS codes, SUM(records) AS records,
               SUM(CASE WHEN mapped THEN records ELSE 0 END) AS mapped
        FROM etl.source_coverage GROUP BY ALL ORDER BY site, records DESC""")
    d["one_to_many"] = _rows(con, """
        SELECT condition_source_value AS source_code, COUNT(DISTINCT condition_concept_id) AS targets,
               STRING_AGG(DISTINCT c.concept_name, ' + ') AS standard_concepts
        FROM cdm.condition_occurrence co JOIN cdm.concept c ON c.concept_id = co.condition_concept_id
        GROUP BY 1 HAVING COUNT(DISTINCT condition_concept_id) > 1""")
    rc = review_csv()
    d["review"] = list(csv.DictReader(open(rc))) if rc.exists() else []

    d["dq"] = _rows(con, "SELECT * FROM results.dq_check_results ORDER BY status DESC, flagged DESC, category")

    d["age"] = _rows(con, f"""
        SELECT {SITE_OF} AS site, LEAST(85, FLOOR((2025 - year_of_birth) / 5) * 5) AS band, COUNT(*) AS n
        FROM cdm.person p WHERE year_of_birth < 2025 GROUP BY ALL ORDER BY 1, 2""")
    d["sex"] = _rows(con, f"""SELECT {SITE_OF} AS site, c.concept_name AS sex, COUNT(*) AS n FROM cdm.person p
        JOIN cdm.concept c ON c.concept_id = p.gender_concept_id GROUP BY ALL ORDER BY 1, 2""")
    d["visits"] = _rows(con, f"""SELECT {SITE_OF} AS site, c.concept_name AS visit, COUNT(*) AS n
        FROM cdm.visit_occurrence v JOIN cdm.person p USING (person_id)
        JOIN cdm.concept c ON c.concept_id = v.visit_concept_id GROUP BY ALL ORDER BY 1, 3 DESC""")
    d["conditions"] = _rows(con, f"""SELECT {SITE_OF} AS site, c.concept_name AS condition,
        COUNT(DISTINCT co.person_id) AS persons FROM cdm.condition_occurrence co JOIN cdm.person p USING (person_id)
        JOIN cdm.concept c ON c.concept_id = co.condition_concept_id GROUP BY ALL ORDER BY 1, 3 DESC""")
    d["lab_dist"] = _rows(con, f"""
        SELECT {SITE_OF} AS site, c.concept_code AS code, c.concept_name AS name,
               COALESCE(m.unit_source_value, '') AS unit_received, u.concept_code AS unit_stored, COUNT(*) AS n,
               ROUND(QUANTILE_CONT(value_as_number, 0.05), 2) AS p05, ROUND(QUANTILE_CONT(value_as_number, 0.25), 2) AS p25,
               ROUND(MEDIAN(value_as_number), 2) AS p50, ROUND(QUANTILE_CONT(value_as_number, 0.75), 2) AS p75,
               ROUND(QUANTILE_CONT(value_as_number, 0.95), 2) AS p95
        FROM cdm.measurement m JOIN cdm.person p USING (person_id)
        JOIN cdm.concept c ON c.concept_id = m.measurement_concept_id
        LEFT JOIN cdm.concept u ON u.concept_id = m.unit_concept_id
        WHERE m.value_as_number IS NOT NULL AND c.vocabulary_id = 'LOINC'
        GROUP BY ALL ORDER BY code, site, unit_received""")
    con.close()

    d["study"] = json.loads((paths.reports / "study_results.json").read_text())
    d["lineage"] = [dict(zip(["table", "field", "cvh", "rch", "rule"], r, strict=True)) for r in LINEAGE]
    return d


def build_site(paths: Paths) -> Path:
    data = collect(paths)
    html = TEMPLATE.read_text().replace("/*__DATA__*/null", json.dumps(data, default=str))
    paths.site.mkdir(parents=True, exist_ok=True)
    out = paths.site / "index.html"
    out.write_text(html)
    report = paths.reports / "study_report.md"
    if report.exists():
        (paths.site / "study_report.md").write_text(report.read_text())
    return out
