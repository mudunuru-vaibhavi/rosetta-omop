"""Step 3: load the OMOP Parquet output and the vocabulary into a DuckDB warehouse.

DuckDB is an in-process analytical SQL database (think "SQLite for analytics"). It reads Parquet
directly, so the Spark output becomes queryable tables in seconds. In production the same CDM would
sit in Postgres, BigQuery, Redshift, Snowflake or Databricks; the SQL in dq/ and study/ is plain
ANSI-style SQL that ports with little change.

Schemas
    cdm      OMOP CDM v5.4 clinical tables + vocabulary tables (the usual OHDSI "cdm schema")
    etl      ETL bookkeeping (source-code coverage)
    results  outputs of data-quality checks and the study
Derived tables built here with SQL: DRUG_ERA and CONDITION_ERA (OHDSI's 30-day persistence window).
"""

from __future__ import annotations

from pathlib import Path

import duckdb

CDM_TABLES = ["person", "observation_period", "visit_occurrence", "condition_occurrence", "drug_exposure",
              "procedure_occurrence", "measurement", "observation", "death", "location", "care_site",
              "provider", "cdm_source"]
VOCAB_TABLES = {
    "concept": "concept_id BIGINT, concept_name VARCHAR, domain_id VARCHAR, vocabulary_id VARCHAR, "
               "concept_class_id VARCHAR, standard_concept VARCHAR, concept_code VARCHAR, "
               "valid_start_date VARCHAR, valid_end_date VARCHAR, invalid_reason VARCHAR",
    "concept_relationship": "concept_id_1 BIGINT, concept_id_2 BIGINT, relationship_id VARCHAR, "
                            "valid_start_date VARCHAR, valid_end_date VARCHAR, invalid_reason VARCHAR",
    "concept_ancestor": "ancestor_concept_id BIGINT, descendant_concept_id BIGINT, "
                        "min_levels_of_separation INTEGER, max_levels_of_separation INTEGER",
    "vocabulary": "vocabulary_id VARCHAR, vocabulary_name VARCHAR, vocabulary_reference VARCHAR, "
                  "vocabulary_version VARCHAR, vocabulary_concept_id BIGINT",
    "domain": "domain_id VARCHAR, domain_name VARCHAR, domain_concept_id BIGINT",
}

ERA_SQL = """
-- Gaps-and-islands: exposures for the same person + ingredient that start within 30 days of the
-- previous one's end are merged into one era (OHDSI's standard persistence window).
CREATE OR REPLACE TABLE cdm.drug_era AS
WITH ing AS (
    SELECT de.person_id, ca.ancestor_concept_id AS ingredient_concept_id,
           de.drug_exposure_start_date AS s, de.drug_exposure_end_date AS e
    FROM cdm.drug_exposure de
    JOIN cdm.concept_ancestor ca ON ca.descendant_concept_id = de.drug_concept_id
    JOIN cdm.concept c ON c.concept_id = ca.ancestor_concept_id
     AND c.concept_class_id = 'Ingredient' AND c.vocabulary_id IN ('RxNorm', 'RxNorm Extension')
    WHERE de.drug_concept_id <> 0
), ordered AS (
    SELECT *, MAX(e) OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY s, e
                           ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_end
    FROM ing
), grouped AS (
    SELECT *, SUM(CASE WHEN prev_end IS NULL OR s > prev_end + 30 THEN 1 ELSE 0 END)
              OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY s, e ROWS UNBOUNDED PRECEDING) AS era
    FROM ordered
)
SELECT ROW_NUMBER() OVER (ORDER BY person_id, ingredient_concept_id, era) AS drug_era_id,
       person_id, ingredient_concept_id AS drug_concept_id,
       MIN(s) AS drug_era_start_date, MAX(e) AS drug_era_end_date, COUNT(*) AS drug_exposure_count,
       GREATEST(0, DATE_DIFF('day', MIN(s), MAX(e)) - SUM(DATE_DIFF('day', s, e))) AS gap_days
FROM grouped GROUP BY person_id, ingredient_concept_id, era;

CREATE OR REPLACE TABLE cdm.condition_era AS
WITH c AS (
    SELECT person_id, condition_concept_id, condition_start_date AS s,
           COALESCE(condition_end_date, condition_start_date) AS e
    FROM cdm.condition_occurrence WHERE condition_concept_id <> 0
), ordered AS (
    SELECT *, MAX(e) OVER (PARTITION BY person_id, condition_concept_id ORDER BY s, e
                           ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_end
    FROM c
), grouped AS (
    SELECT *, SUM(CASE WHEN prev_end IS NULL OR s > prev_end + 30 THEN 1 ELSE 0 END)
              OVER (PARTITION BY person_id, condition_concept_id ORDER BY s, e ROWS UNBOUNDED PRECEDING) AS era
    FROM ordered
)
SELECT ROW_NUMBER() OVER (ORDER BY person_id, condition_concept_id, era) AS condition_era_id,
       person_id, condition_concept_id, MIN(s) AS condition_era_start_date, MAX(e) AS condition_era_end_date,
       COUNT(*) AS condition_occurrence_count
FROM grouped GROUP BY person_id, condition_concept_id, era;
"""


def load_warehouse(db_path: Path, omop_dir: Path, vocab_dir: Path, review_csv: Path) -> dict[str, int]:
    if db_path.exists():
        db_path.unlink()
    con = duckdb.connect(str(db_path))
    for schema in ("cdm", "etl", "results"):
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    for table, cols in VOCAB_TABLES.items():
        columns = dict(c.split(" ", 1) for c in cols.split(", "))
        con.execute(f"CREATE TABLE cdm.{table} AS SELECT * FROM read_csv(?, delim='\t', header=true, "
                    f"quote='', columns={columns!r})", [str(vocab_dir / f"{table.upper()}.csv")])

    for table in CDM_TABLES:
        con.execute(f"CREATE TABLE cdm.{table} AS SELECT * FROM read_parquet(?)", [str(omop_dir / table / "*.parquet")])
    con.execute("CREATE TABLE etl.source_coverage AS SELECT * FROM read_parquet(?)",
                [str(omop_dir / "_etl_source_coverage" / "*.parquet")])

    # SOURCE_TO_CONCEPT_MAP: the OMOP table for reviewed local-code mappings
    con.execute("""
        CREATE TABLE cdm.source_to_concept_map (source_code VARCHAR, source_concept_id BIGINT,
            source_vocabulary_id VARCHAR, source_code_description VARCHAR, target_concept_id BIGINT,
            target_vocabulary_id VARCHAR, valid_start_date DATE, valid_end_date DATE, invalid_reason VARCHAR)""")
    if review_csv.exists():
        con.execute("""
            INSERT INTO cdm.source_to_concept_map
            SELECT r.source_code, 0, r.source_vocabulary_id, r.source_code_description, c.concept_id,
                   r.target_vocabulary_id, CAST(r.reviewed_on AS DATE), DATE '2099-12-31', NULL
            FROM read_csv(?, header=true, all_varchar=true) r
            JOIN cdm.concept c ON c.vocabulary_id = r.target_vocabulary_id AND c.concept_code = r.target_concept_code
            WHERE lower(r.status) = 'approved'""", [str(review_csv)])

    con.execute(ERA_SQL)
    counts = {t: con.execute(f"SELECT COUNT(*) FROM cdm.{t}").fetchone()[0]
              for t in [*CDM_TABLES, "drug_era", "condition_era", "source_to_concept_map", "concept"]}
    con.close()
    return counts


def connect(db_path: Path, read_only: bool = True) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(db_path), read_only=read_only)
