"""Step 5a: build the study cohort from the OMOP CDM with SQL (DuckDB).

Research question
    Among adults with type 2 diabetes starting their first oral glucose-lowering drug, does starting
    glipizide (a sulfonylurea) instead of metformin raise the risk of hypoglycemia?

Design: new-user, active-comparator cohort (the standard RWE design for comparing two drugs)
    * "New user": index date = first-ever exposure to EITHER drug, so both groups start at the same
      point in their disease and nobody is a long-time survivor of the drug ("prevalent user bias").
    * "Active comparator": compare two treatments for the same indication instead of treated vs
      untreated, which reduces confounding by indication.

Concept sets are written as (vocabulary_id, concept_code) and expanded through CONCEPT_ANCESTOR, so a
drug set named by its ingredient automatically includes every clinical drug that contains it. This is
how OHDSI tools (ATLAS) define cohorts, and it means the SQL contains no hard-coded concept ids.
"""

from __future__ import annotations

import duckdb

CONCEPT_SETS = {
    "t2dm": [("SNOMED", "44054006")],
    "metformin": [("RxNorm", "6809")],
    "sulfonylurea": [("RxNorm", "4821")],
    "insulin": [("RxNorm", "274783")],
    "hypoglycemia": [("SNOMED", "302866003")],
    "ckd": [("SNOMED", "433144002")],
    "egfr": [("LOINC", "62238-1"), ("LOINC", "98979-8"), ("LOINC", "33914-3")],
    "hba1c": [("LOINC", "4548-4"), ("LOINC", "17856-6"), ("LOINC", "59261-8")],
}
WASHOUT_DAYS = 365
MAX_FOLLOW_UP_DAYS = 730


def create_concept_sets(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS results")
    con.execute("CREATE OR REPLACE TABLE results.concept_set (set_name VARCHAR, concept_id BIGINT)")
    for name, items in CONCEPT_SETS.items():
        for vocab, code in items:
            con.execute("""
                INSERT INTO results.concept_set
                SELECT DISTINCT ?, ca.descendant_concept_id
                FROM cdm.concept c JOIN cdm.concept_ancestor ca ON ca.ancestor_concept_id = c.concept_id
                WHERE c.vocabulary_id = ? AND c.concept_code = ?""", [name, vocab, code])


COHORT_SQL = f"""
CREATE OR REPLACE TABLE results.study_attrition AS SELECT NULL::VARCHAR AS step, NULL::BIGINT AS persons LIMIT 0;

-- 1. First exposure to metformin or a sulfonylurea per person
CREATE OR REPLACE TEMP TABLE first_exp AS
WITH exp AS (
    SELECT de.person_id, de.drug_exposure_start_date AS d, cs.set_name AS drug
    FROM cdm.drug_exposure de JOIN results.concept_set cs
      ON cs.concept_id = de.drug_concept_id AND cs.set_name IN ('metformin', 'sulfonylurea')
), firsts AS (
    SELECT person_id, MIN(d) AS index_date FROM exp GROUP BY person_id
)
SELECT f.person_id, f.index_date,
       CASE WHEN COUNT(DISTINCT e.drug) > 1 THEN 'both' ELSE MIN(e.drug) END AS arm
FROM firsts f JOIN exp e ON e.person_id = f.person_id AND e.d = f.index_date
GROUP BY f.person_id, f.index_date;
INSERT INTO results.study_attrition SELECT '1. First exposure to metformin or glipizide', COUNT(*) FROM first_exp;

CREATE OR REPLACE TEMP TABLE c1 AS SELECT * FROM first_exp WHERE arm <> 'both';
INSERT INTO results.study_attrition SELECT '2. Did not start both drugs on the same day', COUNT(*) FROM c1;

CREATE OR REPLACE TEMP TABLE c2 AS
SELECT c1.*, op.observation_period_end_date AS obs_end FROM c1
JOIN cdm.observation_period op ON op.person_id = c1.person_id
 AND c1.index_date BETWEEN op.observation_period_start_date AND op.observation_period_end_date
WHERE DATE_DIFF('day', op.observation_period_start_date, c1.index_date) >= {WASHOUT_DAYS};
INSERT INTO results.study_attrition SELECT '3. At least {WASHOUT_DAYS} days of prior observation (new user)', COUNT(*) FROM c2;

CREATE OR REPLACE TEMP TABLE c3 AS
SELECT c2.* FROM c2 WHERE EXISTS (
    SELECT 1 FROM cdm.condition_occurrence co JOIN results.concept_set cs
      ON cs.concept_id = co.condition_concept_id AND cs.set_name = 't2dm'
    WHERE co.person_id = c2.person_id AND co.condition_start_date <= c2.index_date);
INSERT INTO results.study_attrition SELECT '4. Type 2 diabetes diagnosis on or before index', COUNT(*) FROM c3;

CREATE OR REPLACE TEMP TABLE c4 AS
SELECT c3.* FROM c3 WHERE NOT EXISTS (
    SELECT 1 FROM cdm.drug_exposure de JOIN results.concept_set cs
      ON cs.concept_id = de.drug_concept_id AND cs.set_name = 'insulin'
    WHERE de.person_id = c3.person_id AND de.drug_exposure_start_date < c3.index_date);
INSERT INTO results.study_attrition SELECT '5. No insulin before index', COUNT(*) FROM c4;

CREATE OR REPLACE TEMP TABLE c5 AS
SELECT c4.* FROM c4 WHERE NOT EXISTS (
    SELECT 1 FROM cdm.condition_occurrence co JOIN results.concept_set cs
      ON cs.concept_id = co.condition_concept_id AND cs.set_name = 'hypoglycemia'
    WHERE co.person_id = c4.person_id AND co.condition_start_date BETWEEN c4.index_date - {WASHOUT_DAYS}
          AND c4.index_date);
INSERT INTO results.study_attrition SELECT '6. No hypoglycemia in the prior year', COUNT(*) FROM c5;

CREATE OR REPLACE TEMP TABLE c6 AS
SELECT c5.*, p.year_of_birth, p.gender_concept_id, split_part(p.person_source_value, '|', 1) AS site,
       p.person_source_value,
       DATE_DIFF('day', CAST(p.birth_datetime AS DATE), c5.index_date) / 365.25 AS age
FROM c5 JOIN cdm.person p USING (person_id)
WHERE DATE_DIFF('day', CAST(p.birth_datetime AS DATE), c5.index_date) / 365.25 >= 18;
INSERT INTO results.study_attrition SELECT '7. Age 18 or older at index', COUNT(*) FROM c6;

-- Outcome, censoring and baseline covariates
CREATE OR REPLACE TABLE results.study_cohort AS
WITH outcome AS (
    SELECT c6.person_id, MIN(co.condition_start_date) AS event_date
    FROM c6 JOIN cdm.condition_occurrence co ON co.person_id = c6.person_id
    JOIN results.concept_set cs ON cs.concept_id = co.condition_concept_id AND cs.set_name = 'hypoglycemia'
    WHERE co.condition_start_date > c6.index_date GROUP BY c6.person_id
), ckd AS (
    SELECT DISTINCT c6.person_id FROM c6
    LEFT JOIN cdm.condition_occurrence co ON co.person_id = c6.person_id AND co.condition_start_date <= c6.index_date
    LEFT JOIN results.concept_set cs1 ON cs1.concept_id = co.condition_concept_id AND cs1.set_name = 'ckd'
    LEFT JOIN cdm.measurement m ON m.person_id = c6.person_id
         AND m.measurement_date BETWEEN c6.index_date - {WASHOUT_DAYS} AND c6.index_date
    LEFT JOIN results.concept_set cs2 ON cs2.concept_id = m.measurement_concept_id AND cs2.set_name = 'egfr'
    WHERE cs1.concept_id IS NOT NULL OR (cs2.concept_id IS NOT NULL AND m.value_as_number < 60)
), a1c AS (
    SELECT c6.person_id, arg_max(m.value_as_number, m.measurement_date) AS baseline_a1c
    FROM c6 JOIN cdm.measurement m ON m.person_id = c6.person_id
    JOIN results.concept_set cs ON cs.concept_id = m.measurement_concept_id AND cs.set_name = 'hba1c'
    WHERE m.measurement_date BETWEEN c6.index_date - 180 AND c6.index_date AND m.value_as_number IS NOT NULL
    GROUP BY c6.person_id
)
SELECT c6.person_id, c6.person_source_value, c6.site, c6.index_date,
       CASE WHEN c6.arm = 'sulfonylurea' THEN 1 ELSE 0 END AS sulfonylurea,
       c6.arm, ROUND(c6.age, 1) AS age,
       CASE WHEN g.concept_code = 'F' THEN 1 ELSE 0 END AS female,
       CASE WHEN ckd.person_id IS NOT NULL THEN 1 ELSE 0 END AS ckd,
       a1c.baseline_a1c,
       LEAST(COALESCE(o.event_date, DATE '9999-12-31'), c6.obs_end, COALESCE(d.death_date, DATE '9999-12-31'),
             c6.index_date + {MAX_FOLLOW_UP_DAYS}) AS end_date,
       CASE WHEN o.event_date IS NOT NULL AND o.event_date <= LEAST(c6.obs_end,
            COALESCE(d.death_date, DATE '9999-12-31'), c6.index_date + {MAX_FOLLOW_UP_DAYS}) THEN 1 ELSE 0 END AS event
FROM c6
JOIN cdm.concept g ON g.concept_id = c6.gender_concept_id
LEFT JOIN outcome o USING (person_id)
LEFT JOIN ckd USING (person_id)
LEFT JOIN a1c USING (person_id)
LEFT JOIN cdm.death d USING (person_id);
"""


def build_cohort(con: duckdb.DuckDBPyConnection):
    create_concept_sets(con)
    con.execute(COHORT_SQL)
    con.execute("""CREATE OR REPLACE TABLE results.study_cohort AS
                   SELECT *, DATE_DIFF('day', index_date, end_date) AS time_days FROM results.study_cohort""")
    cohort = con.execute("SELECT * FROM results.study_cohort WHERE time_days > 0").df()
    attrition = con.execute("SELECT * FROM results.study_attrition").df()
    return cohort, attrition
