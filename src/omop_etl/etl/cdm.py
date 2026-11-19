"""Step 2 of the ETL: staging -> OMOP CDM v5.4 tables (PySpark).

What happens here, in order:

1. Vocabulary: build one "source map" DataFrame of
       (vocabulary_id, code) -> source_concept_id, target (standard) concept_id, target domain
   from CONCEPT + CONCEPT_RELATIONSHIP ("Maps to") plus the human-approved rows of the
   source_to_concept_map review file (for local codes that no public vocabulary knows).
2. Harmonise units (ounces/pounds -> kg, inches -> cm, HbA1c mmol/mol -> %).
3. Map every staged event. One source code can map to several standard concepts (E11.649 ->
   diabetes AND hypoglycemia), so a join can return several rows: that is intended.
4. Route by DOMAIN of the standard concept, not by where the data came from. A diagnosis code whose
   standard concept is in the Observation domain lands in OBSERVATION; a lab-order CPT code whose
   concept is a Measurement lands in MEASUREMENT.
5. Assign deterministic ids (hash of site + source key) so a re-run produces the same ids and two
   sites can never collide.
6. Write each CDM table as Parquet.
"""

from __future__ import annotations

import csv
import json
from datetime import date
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from . import crosswalks as cw

MAX_ID = 2**53 - 1  # keep ids exact in JavaScript (dashboard) and in every SQL engine


def sid(*parts: Column | str) -> Column:
    """Deterministic surrogate key: positive 53-bit hash of the given parts."""
    cols = [F.coalesce((p if isinstance(p, Column) else F.lit(p)).cast("string"), F.lit("")) for p in parts]
    return F.pmod(F.xxhash64(*cols), F.lit(MAX_ID))


# ---------------------------------------------------------------------------------------------
# Vocabulary
# ---------------------------------------------------------------------------------------------
def load_vocab(spark: SparkSession, vocab_dir: Path) -> dict[str, DataFrame]:
    def read(t: str) -> DataFrame:
        # Athena files are tab-delimited without quoting; disable the quote character
        return spark.read.csv(str(vocab_dir / f"{t}.csv"), sep="\t", header=True, quote="\u0000")

    concept = read("CONCEPT").withColumn("concept_id", F.col("concept_id").cast("long"))
    rel = (read("CONCEPT_RELATIONSHIP")
           .withColumn("concept_id_1", F.col("concept_id_1").cast("long"))
           .withColumn("concept_id_2", F.col("concept_id_2").cast("long")))
    return {"concept": concept, "concept_relationship": rel, "vocabulary": read("VOCABULARY")}


def _valid(col: str) -> Column:
    return F.coalesce(F.col(col), F.lit("")) == ""


def read_review_file(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def build_source_map(spark: SparkSession, vocab: dict[str, DataFrame], review_rows: list[dict]) -> DataFrame:
    concept, rel = vocab["concept"], vocab["concept_relationship"]
    targets = concept.filter((F.col("standard_concept") == "S") & _valid("invalid_reason")).select(
        F.col("concept_id").alias("target_concept_id"), F.col("domain_id").alias("target_domain_id"),
        F.col("vocabulary_id").alias("t_vocab"), F.col("concept_code").alias("t_code"))
    maps_to = rel.filter((F.col("relationship_id") == "Maps to") & _valid("invalid_reason"))
    from_vocab = (
        concept.select(F.col("vocabulary_id"), F.col("concept_code").alias("code"),
                       F.col("concept_id").alias("source_concept_id"))
        .join(maps_to, F.col("source_concept_id") == F.col("concept_id_1"))
        .join(targets, F.col("concept_id_2") == F.col("target_concept_id"))
        .select("vocabulary_id", "code", "source_concept_id", "target_concept_id", "target_domain_id",
                F.lit("vocabulary").alias("mapped_via"))
    )
    approved = [r for r in review_rows if r.get("status", "").lower() == "approved"]
    if not approved:
        return from_vocab
    stcm = spark.createDataFrame(
        [(r["source_vocabulary_id"], r["source_code"], r["target_vocabulary_id"], r["target_concept_code"])
         for r in approved], "vocabulary_id string, code string, t_vocab string, t_code string")
    from_stcm = stcm.join(targets, ["t_vocab", "t_code"]).select(
        "vocabulary_id", "code", F.lit(0).cast("long").alias("source_concept_id"), "target_concept_id",
        "target_domain_id", F.lit("source_to_concept_map").alias("mapped_via"))
    return from_vocab.unionByName(from_stcm)


def fixed_concepts(vocab: dict[str, DataFrame]) -> DataFrame:
    """Small lookup of the closed-list vocabularies used by the crosswalks (gender, race, visit, unit...)."""
    small = ["Gender", "Race", "Ethnicity", "Visit", "CMS Place of Service", "Type Concept",
             "Meas Value Operator", "UCUM"]
    return vocab["concept"].filter(F.col("vocabulary_id").isin(small)).select(
        F.col("vocabulary_id").alias("fx_vocab"), F.col("concept_code").alias("fx_code"),
        F.col("concept_id").alias("fx_id"))


def _lookup(df: DataFrame, fixed: DataFrame, vocab: Column | str, code: Column | str, out: str) -> DataFrame:
    vocab_col = vocab if isinstance(vocab, Column) else F.lit(vocab)
    code_col = code if isinstance(code, Column) else F.col(code)
    fx = F.broadcast(fixed.withColumnRenamed("fx_id", out))
    return (df.withColumn("_v", vocab_col).withColumn("_c", code_col)
            .join(fx, (F.col("_v") == F.col("fx_vocab")) & (F.col("_c") == F.col("fx_code")), "left")
            .withColumn(out, F.coalesce(F.col(out), F.lit(0).cast("long")))
            .drop("_v", "_c", "fx_vocab", "fx_code"))


def _type_id(fixed: DataFrame, key: tuple[str, str]) -> int:
    row = fixed.filter((F.col("fx_vocab") == key[0]) & (F.col("fx_code") == key[1])).first()
    return int(row["fx_id"]) if row else 0


# ---------------------------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------------------------
def _read_staging(spark: SparkSession, staging_dirs: list[Path], name: str) -> DataFrame:
    frames = [spark.read.parquet(str(d / name)) for d in staging_dirs]
    out = frames[0]
    for f in frames[1:]:
        out = out.unionByName(f)
    return out


def harmonise_units(events: DataFrame, spark: SparkSession) -> DataFrame:
    conv = spark.createDataFrame(
        [(u, c, o, float(m), float(b)) for (u, c), (o, m, b) in cw.UNIT_CONVERSIONS.items()],
        "cv_unit string, cv_code string, cv_out string, cv_mult double, cv_offset double")
    joined = events.join(
        F.broadcast(conv),
        (F.col("unit_code") == F.col("cv_unit")) & ((F.col("cv_code") == "*") | (F.col("cv_code") == F.col("code"))),
        "left")
    return (joined
            .withColumn("unit_source_value", F.col("unit_code"))
            .withColumn("value_as_number", F.when(F.col("cv_unit").isNotNull(),
                                                  F.round(F.col("value_as_number") * F.col("cv_mult")
                                                          + F.col("cv_offset"), 2))
                        .otherwise(F.col("value_as_number")))
            .withColumn("unit_code", F.coalesce(F.col("cv_out"), F.col("unit_code")))
            .drop("cv_unit", "cv_code", "cv_out", "cv_mult", "cv_offset"))


def build_cdm(spark: SparkSession, staging_dirs: list[Path], vocab_dir: Path, review_csv: Path,
              out: Path) -> dict:
    vocab = load_vocab(spark, vocab_dir)
    review = read_review_file(review_csv)
    smap = build_source_map(spark, vocab, review).cache()
    fixed = fixed_concepts(vocab).cache()
    t_ehr, t_period = _type_id(fixed, cw.TYPE_EHR), _type_id(fixed, cw.TYPE_PERIOD)

    person_s = _read_staging(spark, staging_dirs, "stg_person")
    visit_s = _read_staging(spark, staging_dirs, "stg_visit")
    event_s = _read_staging(spark, staging_dirs, "stg_event")
    care_s = _read_staging(spark, staging_dirs, "stg_care_site")
    prov_s = _read_staging(spark, staging_dirs, "stg_provider")

    def pid(site: str = "site", psv: str = "person_source_value") -> Column:
        return sid(F.col(site), "person", F.col(psv))

    tables: dict[str, DataFrame] = {}

    # --- location / care_site / provider -----------------------------------------------------
    tables["location"] = person_s.select("site", "zip", "state").dropDuplicates(["site", "zip"]).select(
        sid(F.col("site"), "loc", F.col("zip")).alias("location_id"), F.col("state"), F.col("zip"),
        F.lit("US").alias("country_source_value"),
        F.concat_ws("|", "site", "zip").alias("location_source_value"))
    tables["care_site"] = care_s.select(
        sid(F.col("site"), "cs", F.col("care_site_source_value")).alias("care_site_id"),
        F.col("care_site_name"), F.lit(0).cast("long").alias("place_of_service_concept_id"),
        F.lit(None).cast("long").alias("location_id"),
        F.concat_ws("|", "site", "care_site_source_value").alias("care_site_source_value"),
        F.lit(None).cast("string").alias("place_of_service_source_value"))
    tables["provider"] = prov_s.select(
        sid(F.col("site"), "prov", F.col("provider_source_value")).alias("provider_id"),
        F.col("provider_name"), F.lit(None).cast("string").alias("npi"),
        F.lit(0).cast("long").alias("specialty_concept_id"), F.lit(None).cast("long").alias("care_site_id"),
        F.concat_ws("|", "site", "provider_source_value").alias("provider_source_value"),
        F.col("specialty_source_value"))

    # --- person / death -------------------------------------------------------------------------
    p = _lookup(person_s, fixed, "Gender", "gender_code", "gender_concept_id")
    p = _lookup(p, fixed, "Race", "race_code", "race_concept_id")
    p = _lookup(p, fixed, "Ethnicity", "ethnicity_code", "ethnicity_concept_id")
    tables["person"] = p.select(
        pid().alias("person_id"), "gender_concept_id",
        F.year("birth_date").alias("year_of_birth"), F.month("birth_date").alias("month_of_birth"),
        F.dayofmonth("birth_date").alias("day_of_birth"), F.to_timestamp("birth_date").alias("birth_datetime"),
        "race_concept_id", "ethnicity_concept_id",
        sid(F.col("site"), "loc", F.col("zip")).alias("location_id"),
        F.lit(None).cast("long").alias("provider_id"), F.lit(None).cast("long").alias("care_site_id"),
        F.concat_ws("|", "site", "person_source_value").alias("person_source_value"),
        F.col("gender_source_value"), F.lit(0).cast("long").alias("gender_source_concept_id"),
        F.col("race_source_value"), F.lit(0).cast("long").alias("race_source_concept_id"),
        F.col("ethnicity_source_value"), F.lit(0).cast("long").alias("ethnicity_source_concept_id"))
    tables["death"] = person_s.filter(F.col("death_date").isNotNull()).select(
        pid().alias("person_id"), F.col("death_date"), F.to_timestamp("death_date").alias("death_datetime"),
        F.lit(t_ehr).cast("long").alias("death_type_concept_id"), F.lit(0).cast("long").alias("cause_concept_id"),
        F.lit(None).cast("string").alias("cause_source_value"),
        F.lit(0).cast("long").alias("cause_source_concept_id"))

    # --- visit_occurrence ---------------------------------------------------------------------
    v = _lookup(visit_s, fixed, F.col("visit_vocab"), F.col("visit_code"), "visit_concept_id")
    tables["visit_occurrence"] = v.select(
        sid(F.col("site"), "visit", F.col("visit_source_value")).alias("visit_occurrence_id"),
        pid().alias("person_id"), "visit_concept_id",
        F.to_date("start_dt").alias("visit_start_date"), F.col("start_dt").alias("visit_start_datetime"),
        F.to_date("end_dt").alias("visit_end_date"), F.col("end_dt").alias("visit_end_datetime"),
        F.lit(t_ehr).cast("long").alias("visit_type_concept_id"),
        F.when(F.col("provider_source_value").isNotNull(),
               sid(F.col("site"), "prov", F.col("provider_source_value"))).alias("provider_id"),
        sid(F.col("site"), "cs", F.col("care_site_source_value")).alias("care_site_id"),
        F.concat_ws("|", "site", "visit_source_value", "visit_source_text").alias("visit_source_value"),
        F.lit(0).cast("long").alias("visit_source_concept_id"),
        F.lit(0).cast("long").alias("admitted_from_concept_id"),
        F.lit(0).cast("long").alias("discharged_to_concept_id"),
        F.lit(None).cast("long").alias("preceding_visit_occurrence_id"))

    # --- clinical events: harmonise, map, route ------------------------------------------------
    ev = harmonise_units(event_s, spark)
    src_concepts = vocab["concept"].select(F.col("vocabulary_id").alias("sc_vocab"),
                                           F.col("concept_code").alias("sc_code"),
                                           F.col("concept_id").alias("sc_id"))
    default_domain = F.lit(None).cast("string")
    for k, d in cw.DEFAULT_DOMAIN.items():
        default_domain = F.when(F.col("event_kind") == k, d).otherwise(default_domain)
    mapped = (
        ev.join(smap, ["vocabulary_id", "code"], "left")
        .join(src_concepts, (F.col("vocabulary_id") == F.col("sc_vocab")) & (F.col("code") == F.col("sc_code")),
              "left")
        .withColumn("concept_id", F.coalesce(F.col("target_concept_id"), F.lit(0).cast("long")))
        .withColumn("domain", F.coalesce(F.col("target_domain_id"), default_domain))
        .withColumn("source_concept_id", F.coalesce(F.col("source_concept_id"), F.col("sc_id"),
                                                    F.lit(0).cast("long")))
        .withColumn("event_id", sid(F.col("site"), F.col("event_source_id"), F.col("concept_id")))
        .withColumn("person_id", pid())
        .withColumn("visit_occurrence_id", F.when(F.col("visit_source_value").isNotNull(),
                                                  sid(F.col("site"), "visit", F.col("visit_source_value"))))
        .withColumn("provider_id", F.when(F.col("provider_source_value").isNotNull(),
                                          sid(F.col("site"), "prov", F.col("provider_source_value"))))
        .withColumn("start_date", F.to_date("start_dt"))
    )
    mapped = _lookup(mapped, fixed, "UCUM", "unit_code", "unit_concept_id")
    mapped = _lookup(mapped, fixed, "Meas Value Operator", "operator", "operator_concept_id")
    mapped = mapped.withColumn("operator_concept_id",
                               F.when(F.col("operator_concept_id") == 0, None).otherwise(F.col("operator_concept_id")))
    mapped = mapped.cache()
    T = F.lit(t_ehr).cast("long")

    tables["condition_occurrence"] = mapped.filter(F.col("domain") == "Condition").select(
        F.col("event_id").alias("condition_occurrence_id"), "person_id",
        F.col("concept_id").alias("condition_concept_id"), F.col("start_date").alias("condition_start_date"),
        F.col("start_dt").alias("condition_start_datetime"), F.lit(None).cast("date").alias("condition_end_date"),
        T.alias("condition_type_concept_id"), F.lit(0).cast("long").alias("condition_status_concept_id"),
        "provider_id", "visit_occurrence_id", F.col("code").alias("condition_source_value"),
        F.col("source_concept_id").alias("condition_source_concept_id"))

    tables["observation"] = mapped.filter(F.col("domain") == "Observation").select(
        F.col("event_id").alias("observation_id"), "person_id", F.col("concept_id").alias("observation_concept_id"),
        F.col("start_date").alias("observation_date"), F.col("start_dt").alias("observation_datetime"),
        T.alias("observation_type_concept_id"), "value_as_number",
        F.col("value_source_value").alias("value_as_string"), F.lit(0).cast("long").alias("value_as_concept_id"),
        F.lit(0).cast("long").alias("unit_concept_id"), "provider_id", "visit_occurrence_id",
        F.col("code").alias("observation_source_value"),
        F.col("source_concept_id").alias("observation_source_concept_id"))

    tables["measurement"] = mapped.filter(F.col("domain") == "Measurement").select(
        F.col("event_id").alias("measurement_id"), "person_id", F.col("concept_id").alias("measurement_concept_id"),
        F.col("start_date").alias("measurement_date"), F.col("start_dt").alias("measurement_datetime"),
        T.alias("measurement_type_concept_id"), "operator_concept_id", "value_as_number",
        F.lit(0).cast("long").alias("value_as_concept_id"),
        F.when(F.col("value_as_number").isNotNull(), F.col("unit_concept_id")).otherwise(F.lit(0).cast("long"))
        .alias("unit_concept_id"),
        F.lit(None).cast("double").alias("range_low"), F.lit(None).cast("double").alias("range_high"),
        "provider_id", "visit_occurrence_id", F.col("code").alias("measurement_source_value"),
        F.col("source_concept_id").alias("measurement_source_concept_id"), "unit_source_value",
        "value_source_value")

    tables["procedure_occurrence"] = mapped.filter(F.col("domain") == "Procedure").select(
        F.col("event_id").alias("procedure_occurrence_id"), "person_id",
        F.col("concept_id").alias("procedure_concept_id"), F.col("start_date").alias("procedure_date"),
        F.col("start_dt").alias("procedure_datetime"), T.alias("procedure_type_concept_id"),
        F.lit(0).cast("long").alias("modifier_concept_id"), F.lit(None).cast("int").alias("quantity"),
        "provider_id", "visit_occurrence_id", F.col("code").alias("procedure_source_value"),
        F.col("source_concept_id").alias("procedure_source_concept_id"))

    end = F.coalesce(F.col("end_date"), F.date_add("start_date", F.coalesce(F.col("days_supply"), F.lit(1))))
    tables["drug_exposure"] = mapped.filter(F.col("domain") == "Drug").select(
        F.col("event_id").alias("drug_exposure_id"), "person_id", F.col("concept_id").alias("drug_concept_id"),
        F.col("start_date").alias("drug_exposure_start_date"), F.col("start_dt").alias("drug_exposure_start_datetime"),
        end.alias("drug_exposure_end_date"), F.col("end_date").alias("verbatim_end_date"),
        T.alias("drug_type_concept_id"), "refills", "quantity", "days_supply",
        F.lit(None).cast("string").alias("sig"), F.lit(0).cast("long").alias("route_concept_id"),
        "provider_id", "visit_occurrence_id", F.col("code").alias("drug_source_value"),
        F.col("source_concept_id").alias("drug_source_concept_id"))

    # --- observation_period: first to last recorded activity (the usual rule for EHR data) -------
    spans = (
        tables["visit_occurrence"].select("person_id", F.col("visit_start_date").alias("d1"),
                                          F.coalesce("visit_end_date", "visit_start_date").alias("d2"))
        .unionByName(mapped.select("person_id", F.col("start_date").alias("d1"), F.col("start_date").alias("d2")))
        .unionByName(tables["drug_exposure"].select("person_id", F.col("drug_exposure_start_date").alias("d1"),
                                                    F.col("drug_exposure_end_date").alias("d2")))
    )
    tables["observation_period"] = spans.groupBy("person_id").agg(
        F.min("d1").alias("observation_period_start_date"), F.max("d2").alias("observation_period_end_date")
    ).select(F.col("person_id").alias("observation_period_id"), "person_id", "observation_period_start_date",
             "observation_period_end_date", F.lit(t_period).cast("long").alias("period_type_concept_id"))

    # OMOP convention: a person with no clinical records (so no observation period) is not loaded.
    observed = tables["observation_period"].select("person_id")
    tables["_etl_dropped_persons"] = tables["person"].join(observed, "person_id", "left_anti").select(
        "person_id", "person_source_value", F.lit("no clinical records").alias("reason"))
    tables["person"] = tables["person"].join(observed, "person_id", "left_semi")
    tables["death"] = tables["death"].join(observed, "person_id", "left_semi")

    vocab_version = vocab["vocabulary"].select("vocabulary_version").first()[0]
    tables["cdm_source"] = spark.createDataFrame([(
        "Multi-site Research CDM (synthetic demo)", "MSR-DEMO", "Portfolio project",
        "Cedar Valley Health (Epic Clarity) + Riverside Community Health (FHIR R4); fictional, synthetic",
        "https://github.com/", "https://github.com/", date(2026, 1, 1), date.today(), "v5.4", 0, vocab_version)],
        "cdm_source_name string, cdm_source_abbreviation string, cdm_holder string, source_description string, "
        "source_documentation_reference string, cdm_etl_reference string, source_release_date date, "
        "cdm_release_date date, cdm_version string, cdm_version_concept_id long, vocabulary_version string")

    # --- ETL bookkeeping: how well did each source code map? ------------------------------------
    tables["_etl_source_coverage"] = mapped.groupBy(
        "site", "event_kind", "vocabulary_id", "code").agg(
        F.first("source_description", ignorenulls=True).alias("source_description"),
        F.countDistinct("event_source_id").alias("records"),
        F.max(F.col("concept_id") > 0).alias("mapped"),
        F.array_sort(F.collect_set("concept_id")).alias("target_concept_ids"),
        F.first("mapped_via", ignorenulls=True).alias("mapped_via"),
        F.first("unit_source_value", ignorenulls=True).alias("example_unit"))

    out.mkdir(parents=True, exist_ok=True)
    counts = {}
    for name, df in tables.items():
        df.write.mode("overwrite").parquet(str(out / name))
        counts[name] = spark.read.parquet(str(out / name)).count()
    (out / "_counts.json").write_text(json.dumps(counts, indent=2))
    mapped.unpersist()
    smap.unpersist()
    return counts
