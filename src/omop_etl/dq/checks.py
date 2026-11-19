"""Step 4: data-quality checks on the OMOP warehouse (DuckDB SQL).

The checks follow the design of OHDSI's Data Quality Dashboard (DQD), which is built on the
Kahn harmonized data-quality framework (Kahn et al., eGEMs 2016):

    Conformance   Do values follow the model's rules?    (types, keys, required fields, domains)
    Completeness  Is the data there?                      (missing values, unmapped codes)
    Plausibility  Do values make clinical sense?           (ranges, dates, sex-specific conditions)

Each check counts the rows that violate a rule (numerator) out of the rows it applies to
(denominator). The check FAILS when the violation percentage is above its threshold. A check that
passes but still found violating rows is reported as "passed with flags" so nothing is hidden.

Check names reuse DQD's names (isPrimaryKey, isForeignKey, fkDomain, isStandardValidConcept,
plausibleValueLow/High, plausibleTemporalAfter, plausibleBeforeDeath, plausibleStartBeforeEnd,
plausibleGender, plausibleUnitConceptIds, standardConceptRecordCompleteness, ...). The thresholds are
this project's choices. Two checks are custom for a multi-site CDM: per-site mapping coverage and
cross-site distribution consistency.

severity "fatal" checks (duplicate keys, orphan records) stop the pipeline; everything else is reported.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

import duckdb


@dataclass
class Check:
    check_name: str
    category: str  # Conformance | Completeness | Plausibility
    subcategory: str  # Value | Relational | Computational | Atemporal | Temporal
    context: str  # Verification (internal rules) | Validation (against outside knowledge)
    table: str
    field: str
    description: str
    violated_sql: str  # SELECT COUNT(*) ...
    denominator_sql: str  # SELECT COUNT(*) ...
    threshold_pct: float = 0.0
    severity: str = "warning"  # fatal | warning


@dataclass
class Result:
    check_name: str
    category: str
    subcategory: str
    context: str
    table: str
    field: str
    description: str
    threshold_pct: float
    severity: str
    num_violated: int
    num_denominator: int
    pct_violated: float
    status: str  # PASS | FAIL | NA | ERROR
    flagged: bool
    error: str | None = None


# (table, primary key, event date column, concept column, expected domain)
EVENT_TABLES = [
    ("condition_occurrence", "condition_occurrence_id", "condition_start_date", "condition_concept_id", "Condition"),
    ("drug_exposure", "drug_exposure_id", "drug_exposure_start_date", "drug_concept_id", "Drug"),
    ("measurement", "measurement_id", "measurement_date", "measurement_concept_id", "Measurement"),
    ("observation", "observation_id", "observation_date", "observation_concept_id", "Observation"),
    ("procedure_occurrence", "procedure_occurrence_id", "procedure_date", "procedure_concept_id", "Procedure"),
    ("visit_occurrence", "visit_occurrence_id", "visit_start_date", "visit_concept_id", "Visit"),
]
REQUIRED = {
    "person": ["person_id", "gender_concept_id", "year_of_birth", "race_concept_id", "ethnicity_concept_id"],
    "observation_period": ["person_id", "observation_period_start_date", "observation_period_end_date",
                           "period_type_concept_id"],
    "visit_occurrence": ["person_id", "visit_concept_id", "visit_start_date", "visit_end_date",
                         "visit_type_concept_id"],
    "condition_occurrence": ["person_id", "condition_concept_id", "condition_start_date",
                             "condition_type_concept_id"],
    "drug_exposure": ["person_id", "drug_concept_id", "drug_exposure_start_date", "drug_exposure_end_date",
                      "drug_type_concept_id"],
    "measurement": ["person_id", "measurement_concept_id", "measurement_date", "measurement_type_concept_id"],
    "observation": ["person_id", "observation_concept_id", "observation_date", "observation_type_concept_id"],
    "procedure_occurrence": ["person_id", "procedure_concept_id", "procedure_date", "procedure_type_concept_id"],
}
# (vocabulary, code) -> plausible range in the harmonised unit, expected UCUM unit
LAB_RULES = {
    ("LOINC", "4548-4"): (3, 20, "%"), ("LOINC", "17856-6"): (3, 20, "%"), ("LOINC", "59261-8"): (3, 20, "%"),
    ("LOINC", "2345-7"): (10, 1000, "mg/dL"), ("LOINC", "2339-0"): (10, 1000, "mg/dL"),
    ("LOINC", "41653-7"): (10, 1000, "mg/dL"), ("LOINC", "98979-8"): (2, 200, "mL/min/{1.73_m2}"),
    ("LOINC", "2160-0"): (0.1, 20, "mg/dL"), ("LOINC", "62238-1"): (2, 200, "mL/min/{1.73_m2}"),
    ("LOINC", "13457-7"): (5, 400, "mg/dL"), ("LOINC", "8480-6"): (50, 260, "mm[Hg]"),
    ("LOINC", "8462-4"): (25, 160, "mm[Hg]"), ("LOINC", "29463-7"): (20, 300, "kg"),
    ("LOINC", "8302-2"): (100, 230, "cm"),
}
SEX_SPECIFIC = {("SNOMED", "72892002"): "F", ("SNOMED", "266569009"): "M"}  # pregnancy, BPH


def _site(alias: str = "p") -> str:
    return f"split_part({alias}.person_source_value, '|', 1)"


def build_checks() -> list[Check]:
    c: list[Check] = []
    cnt = "SELECT COUNT(*) FROM "

    # ---------------- Conformance -----------------------------------------------------------
    for table, pk, *_ in EVENT_TABLES + [("person", "person_id", None, None, None)]:
        c.append(Check("isPrimaryKey", "Conformance", "Relational", "Verification", table, pk,
                       f"{pk} is unique",
                       f"SELECT COUNT(*) - COUNT(DISTINCT {pk}) FROM cdm.{table}", f"{cnt} cdm.{table}",
                       0, "fatal"))
    for table, cols in REQUIRED.items():
        for col in cols:
            c.append(Check("isRequired", "Conformance", "Relational", "Verification", table, col,
                           f"{col} is never NULL", f"{cnt} cdm.{table} WHERE {col} IS NULL", f"{cnt} cdm.{table}",
                           0, "fatal" if col == "person_id" else "warning"))
    for table, *_ in EVENT_TABLES + [("observation_period",), ("death",)]:
        c.append(Check("isForeignKey", "Conformance", "Relational", "Verification", table, "person_id",
                       "every row belongs to a person in PERSON",
                       f"{cnt} cdm.{table} t WHERE NOT EXISTS (SELECT 1 FROM cdm.person p WHERE p.person_id = t.person_id)",
                       f"{cnt} cdm.{table}", 0, "fatal"))
    for table, _, _, _, _ in EVENT_TABLES[:-1]:
        c.append(Check("isForeignKey", "Conformance", "Relational", "Verification", table, "visit_occurrence_id",
                       "visit_occurrence_id points at an existing visit",
                       f"{cnt} cdm.{table} t WHERE t.visit_occurrence_id IS NOT NULL AND NOT EXISTS "
                       f"(SELECT 1 FROM cdm.visit_occurrence v WHERE v.visit_occurrence_id = t.visit_occurrence_id)",
                       f"{cnt} cdm.{table} WHERE visit_occurrence_id IS NOT NULL", 0))
    concept_fields = [(t, col, dom) for t, _, _, col, dom in EVENT_TABLES] + [
        ("person", "gender_concept_id", "Gender"), ("measurement", "unit_concept_id", "Unit")]
    for table, col, dom in concept_fields:
        c.append(Check("isForeignKey", "Conformance", "Relational", "Verification", table, col,
                       f"{col} exists in CONCEPT",
                       f"{cnt} cdm.{table} t LEFT JOIN cdm.concept c ON c.concept_id = t.{col} WHERE c.concept_id IS NULL",
                       f"{cnt} cdm.{table}", 0))
        c.append(Check("fkDomain", "Conformance", "Value", "Verification", table, col,
                       f"{col} is a concept in the {dom} domain",
                       f"{cnt} cdm.{table} t JOIN cdm.concept c ON c.concept_id = t.{col} "
                       f"WHERE t.{col} <> 0 AND c.domain_id <> '{dom}'",
                       f"{cnt} cdm.{table} WHERE {col} <> 0", 0))
        c.append(Check("isStandardValidConcept", "Conformance", "Value", "Verification", table, col,
                       f"{col} is a standard, valid concept",
                       f"{cnt} cdm.{table} t JOIN cdm.concept c ON c.concept_id = t.{col} "
                       f"WHERE t.{col} <> 0 AND (c.standard_concept IS DISTINCT FROM 'S' OR COALESCE(c.invalid_reason, '') <> '')",
                       f"{cnt} cdm.{table} WHERE {col} <> 0", 0))
    for vocab, code in LAB_RULES:
        unit = LAB_RULES[(vocab, code)][2]
        c.append(Check("plausibleUnitConceptIds", "Conformance", "Value", "Validation", "measurement",
                       f"unit_concept_id [{code}]",
                       f"{code} results with a value are stored in {unit}",
                       f"{cnt} cdm.measurement m JOIN cdm.concept c ON c.concept_id = m.measurement_concept_id "
                       f"LEFT JOIN cdm.concept u ON u.concept_id = m.unit_concept_id "
                       f"WHERE c.vocabulary_id = '{vocab}' AND c.concept_code = '{code}' AND m.value_as_number IS NOT NULL "
                       f"AND COALESCE(u.concept_code, '') <> '{unit}'",
                       f"{cnt} cdm.measurement m JOIN cdm.concept c ON c.concept_id = m.measurement_concept_id "
                       f"WHERE c.vocabulary_id = '{vocab}' AND c.concept_code = '{code}' AND m.value_as_number IS NOT NULL",
                       0))

    # ---------------- Completeness ----------------------------------------------------------
    for table, _, _, col, _ in EVENT_TABLES:
        c.append(Check("standardConceptRecordCompleteness", "Completeness", "", "Verification", table, col,
                       f"share of rows with {col} = 0 (not mapped to a standard concept)",
                       f"{cnt} cdm.{table} WHERE {col} = 0", f"{cnt} cdm.{table}", 5))
    for site in ("CVH", "RCH"):
        c.append(Check("sourceCodeMappingCoverage", "Completeness", "", "Verification", "etl.source_coverage",
                       f"site {site}", f"share of {site} source records whose code did not map (custom check)",
                       f"SELECT COALESCE(SUM(records), 0) FROM etl.source_coverage WHERE site = '{site}' AND NOT mapped",
                       f"SELECT COALESCE(SUM(records), 0) FROM etl.source_coverage WHERE site = '{site}'", 5))
    c.append(Check("measurePersonCompleteness", "Completeness", "", "Validation", "observation_period", "person_id",
                   "every person has an observation period",
                   f"{cnt} cdm.person p WHERE NOT EXISTS (SELECT 1 FROM cdm.observation_period o WHERE o.person_id = p.person_id)",
                   f"{cnt} cdm.person", 0))
    c.append(Check("measurePersonCompleteness", "Completeness", "", "Validation", "visit_occurrence", "person_id",
                   "share of persons with no visit", f"{cnt} cdm.person p WHERE NOT EXISTS "
                   f"(SELECT 1 FROM cdm.visit_occurrence v WHERE v.person_id = p.person_id)", f"{cnt} cdm.person", 1))
    c.append(Check("measureValueCompleteness", "Completeness", "", "Verification", "measurement", "value_as_number",
                   "LOINC-coded results that carry no numeric value",
                   f"{cnt} cdm.measurement m JOIN cdm.concept c ON c.concept_id = m.measurement_concept_id "
                   f"WHERE c.vocabulary_id = 'LOINC' AND m.value_as_number IS NULL",
                   f"{cnt} cdm.measurement m JOIN cdm.concept c ON c.concept_id = m.measurement_concept_id "
                   f"WHERE c.vocabulary_id = 'LOINC'", 1))
    c.append(Check("measureValueCompleteness", "Completeness", "", "Verification", "person", "race_concept_id",
                   "share of persons with unknown race (race_concept_id = 0)",
                   f"{cnt} cdm.person WHERE race_concept_id = 0", f"{cnt} cdm.person", 10))

    # ---------------- Plausibility ----------------------------------------------------------
    for (vocab, code), (lo, hi, _) in LAB_RULES.items():
        base = (f"FROM cdm.measurement m JOIN cdm.concept c ON c.concept_id = m.measurement_concept_id "
                f"WHERE c.vocabulary_id = '{vocab}' AND c.concept_code = '{code}' AND m.value_as_number IS NOT NULL")
        c.append(Check("plausibleValueLow", "Plausibility", "Atemporal", "Validation", "measurement",
                       f"value_as_number [{code}]", f"{code} values are not below {lo}",
                       f"SELECT COUNT(*) {base} AND m.value_as_number < {lo}", f"SELECT COUNT(*) {base}", 1))
        c.append(Check("plausibleValueHigh", "Plausibility", "Atemporal", "Validation", "measurement",
                       f"value_as_number [{code}]", f"{code} values are not above {hi}",
                       f"SELECT COUNT(*) {base} AND m.value_as_number > {hi}", f"SELECT COUNT(*) {base}", 1))
    c.append(Check("plausibleValueHigh", "Plausibility", "Atemporal", "Validation", "person", "year_of_birth",
                   "year_of_birth is not in the future", f"{cnt} cdm.person WHERE year_of_birth > year(current_date)",
                   f"{cnt} cdm.person", 1))
    c.append(Check("plausibleValueLow", "Plausibility", "Atemporal", "Validation", "person", "year_of_birth",
                   "year_of_birth is after 1900", f"{cnt} cdm.person WHERE year_of_birth < 1900", f"{cnt} cdm.person", 1))
    for table, _, date_col, _, _ in EVENT_TABLES + [("observation_period", None, "observation_period_start_date",
                                                     None, None)]:
        c.append(Check("plausibleTemporalAfter", "Plausibility", "Temporal", "Verification", table, date_col,
                       f"{date_col} is on or after the person's birth",
                       f"{cnt} cdm.{table} t JOIN cdm.person p USING (person_id) "
                       f"WHERE t.{date_col} < CAST(p.birth_datetime AS DATE)",
                       f"{cnt} cdm.{table}", 1))
        if table != "observation_period":
            c.append(Check("plausibleBeforeDeath", "Plausibility", "Temporal", "Verification", table, date_col,
                           f"{date_col} is no more than 60 days after death",
                           f"{cnt} cdm.{table} t JOIN cdm.death d USING (person_id) "
                           f"WHERE t.{date_col} > d.death_date + 60", f"{cnt} cdm.{table}", 1))
            c.append(Check("withinObservationPeriod", "Plausibility", "Temporal", "Verification", table, date_col,
                           f"{date_col} falls inside an observation period",
                           f"{cnt} cdm.{table} t WHERE NOT EXISTS (SELECT 1 FROM cdm.observation_period o "
                           f"WHERE o.person_id = t.person_id AND t.{date_col} BETWEEN o.observation_period_start_date "
                           f"AND o.observation_period_end_date)", f"{cnt} cdm.{table}", 0))
    for table, s, e in [("visit_occurrence", "visit_start_datetime", "visit_end_datetime"),
                        ("drug_exposure", "drug_exposure_start_date", "drug_exposure_end_date"),
                        ("observation_period", "observation_period_start_date", "observation_period_end_date")]:
        c.append(Check("plausibleStartBeforeEnd", "Plausibility", "Temporal", "Verification", table, e,
                       f"{s} is on or before {e}", f"{cnt} cdm.{table} WHERE {e} < {s}", f"{cnt} cdm.{table}", 1))
    for (vocab, code), sex in SEX_SPECIFIC.items():
        other = "M" if sex == "F" else "F"
        c.append(Check("plausibleGender", "Plausibility", "Atemporal", "Validation", "condition_occurrence",
                       f"condition_concept_id [{code}]", f"{vocab} {code} only occurs in sex {sex}",
                       f"{cnt} cdm.condition_occurrence co JOIN cdm.concept c ON c.concept_id = co.condition_concept_id "
                       f"JOIN cdm.person p USING (person_id) JOIN cdm.concept g ON g.concept_id = p.gender_concept_id "
                       f"WHERE c.vocabulary_id = '{vocab}' AND c.concept_code = '{code}' AND g.concept_code = '{other}'",
                       f"{cnt} cdm.condition_occurrence co JOIN cdm.concept c ON c.concept_id = co.condition_concept_id "
                       f"WHERE c.vocabulary_id = '{vocab}' AND c.concept_code = '{code}'", 1))
    # Custom multi-site check: the median of each lab should be similar across sites after harmonisation.
    for (vocab, code) in [("LOINC", "4548-4"), ("LOINC", "29463-7"), ("LOINC", "8302-2"), ("LOINC", "2345-7")]:
        c.append(Check("crossSiteDistribution", "Plausibility", "Atemporal", "Validation", "measurement",
                       f"value_as_number [{code}]",
                       f"{code} median differs by more than 15% between sites (1 = yes; custom check)",
                       f"""SELECT CASE WHEN MAX(med) > 1.15 * MIN(med) THEN 1 ELSE 0 END FROM (
                             SELECT {_site()} AS site, MEDIAN(m.value_as_number) AS med
                             FROM cdm.measurement m JOIN cdm.concept c ON c.concept_id = m.measurement_concept_id
                             JOIN cdm.person p USING (person_id)
                             WHERE c.vocabulary_id = '{vocab}' AND c.concept_code = '{code}' AND m.value_as_number IS NOT NULL
                             GROUP BY 1)""", "SELECT 1", 0))
    return c


def run_checks(db_path: Path, out_json: Path | None = None) -> list[Result]:
    con = duckdb.connect(str(db_path))
    results: list[Result] = []
    for ch in build_checks():
        try:
            num = int(con.execute(ch.violated_sql).fetchone()[0] or 0)
            den = int(con.execute(ch.denominator_sql).fetchone()[0] or 0)
            pct = round(100.0 * num / den, 4) if den else 0.0
            status = "NA" if den == 0 else ("FAIL" if pct > ch.threshold_pct else "PASS")
            results.append(Result(ch.check_name, ch.category, ch.subcategory, ch.context, ch.table, ch.field,
                                  ch.description, ch.threshold_pct, ch.severity, num, den, pct, status,
                                  status == "PASS" and num > 0))
        except duckdb.Error as e:
            results.append(Result(ch.check_name, ch.category, ch.subcategory, ch.context, ch.table, ch.field,
                                  ch.description, ch.threshold_pct, ch.severity, 0, 0, 0.0, "ERROR", False, str(e)))

    con.execute("CREATE SCHEMA IF NOT EXISTS results")
    con.execute("DROP TABLE IF EXISTS results.dq_check_results")
    con.execute("""CREATE TABLE results.dq_check_results (check_name VARCHAR, category VARCHAR, subcategory VARCHAR,
        context VARCHAR, cdm_table VARCHAR, cdm_field VARCHAR, description VARCHAR, threshold_pct DOUBLE,
        severity VARCHAR, num_violated BIGINT, num_denominator BIGINT, pct_violated DOUBLE, status VARCHAR,
        flagged BOOLEAN, error VARCHAR)""")
    con.executemany("INSERT INTO results.dq_check_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [list(asdict(r).values()) for r in results])
    con.close()
    if out_json:
        out_json.parent.mkdir(parents=True, exist_ok=True)
        out_json.write_text(json.dumps({"run_at": datetime.now().isoformat(timespec="seconds"),
                                        "results": [asdict(r) for r in results]}, indent=1))
    return results


def summarise(results: list[Result]) -> dict:
    by = {}
    for r in results:
        b = by.setdefault(r.category, {"PASS": 0, "FAIL": 0, "NA": 0, "ERROR": 0, "flagged": 0})
        b[r.status] += 1
        b["flagged"] += int(r.flagged)
    fatal = [r for r in results if r.severity == "fatal" and r.status in ("FAIL", "ERROR")]
    return {"total": len(results), "by_category": by, "fatal_failures": len(fatal),
            "failed": [f"{r.check_name} {r.table}.{r.field}: {r.num_violated}/{r.num_denominator}"
                       for r in results if r.status in ("FAIL", "ERROR")]}
