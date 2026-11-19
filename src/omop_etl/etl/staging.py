"""Step 1 of the ETL: reshape each source into one shared staging layout (PySpark).

Every site, whatever its native format, is turned into the same six staging tables. Everything after
this step (vocabulary mapping, unit harmonisation, OMOP table building) is written once and works for
any number of sites. Adding a third health system means writing one new stage_* function.

    stg_person     one row per patient
    stg_visit      one row per encounter
    stg_event      one row per coded clinical fact (diagnosis, procedure, lab, vital, drug)
    stg_care_site  departments / facilities
    stg_provider   clinicians

Nothing is mapped to OMOP concepts here. Codes stay as (vocabulary_id, code) exactly as received.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from . import crosswalks as cw

EVENT_COLUMNS = [
    "site", "event_source_id", "person_source_value", "visit_source_value", "provider_source_value",
    "event_kind", "start_dt", "end_date", "vocabulary_id", "code", "source_description",
    "value_as_number", "value_source_value", "operator", "unit_code", "quantity", "days_supply", "refills",
]


def _map(mapping: dict, col: Column) -> Column:
    """Translate a column through a small Python dict (unknown values become NULL)."""
    expr = F.lit(None).cast("string")
    for k, v in mapping.items():
        expr = F.when(col == F.lit(k), F.lit(v)).otherwise(expr)
    return expr


def _events(df: DataFrame) -> DataFrame:
    """Fill any missing staging column with NULL and put the columns in a fixed order."""
    types = {"start_dt": "timestamp", "end_date": "date", "value_as_number": "double", "quantity": "double",
             "days_supply": "int", "refills": "int"}
    cols = []
    for c in EVENT_COLUMNS:
        if c in df.columns:
            cols.append(F.col(c).cast(types.get(c, "string")).alias(c))
        else:
            cols.append(F.lit(None).cast(types.get(c, "string")).alias(c))
    return df.select(*cols)


# =============================================================================================
# Cedar Valley Health: Epic Clarity pipe-delimited extract
# =============================================================================================
def stage_clarity(spark: SparkSession, src: Path, site: str = "CVH") -> dict[str, DataFrame]:
    def read(table: str) -> DataFrame:
        return spark.read.csv(str(src / f"{table}.txt"), sep="|", header=True, inferSchema=False)

    patient, race, enc, hsp = read("PATIENT"), read("PATIENT_RACE"), read("PAT_ENC"), read("PAT_ENC_HSP")
    zc_sex, zc_race, zc_eth = read("ZC_SEX"), read("ZC_PATIENT_RACE"), read("ZC_ETHNIC_GROUP")

    person = (
        patient.join(race.filter("LINE = '1'"), "PAT_ID", "left")
        .join(zc_sex, "SEX_C", "left").withColumnRenamed("NAME", "sex_name")
        .join(zc_race, "PATIENT_RACE_C", "left").withColumnRenamed("NAME", "race_name")
        .join(zc_eth, "ETHNIC_GROUP_C", "left").withColumnRenamed("NAME", "eth_name")
        .select(
            F.lit(site).alias("site"),
            F.col("PAT_ID").alias("person_source_value"),
            F.to_date("BIRTH_DATE").alias("birth_date"),
            _map(cw.CLARITY_SEX, F.col("SEX_C")).alias("gender_code"),
            F.col("sex_name").alias("gender_source_value"),
            _map(cw.CLARITY_RACE, F.col("PATIENT_RACE_C")).alias("race_code"),
            F.col("race_name").alias("race_source_value"),
            _map(cw.CLARITY_ETHNICITY, F.col("ETHNIC_GROUP_C")).alias("ethnicity_code"),
            F.col("eth_name").alias("ethnicity_source_value"),
            F.col("ZIP").alias("zip"), F.col("STATE").alias("state"),
            F.to_date(F.nullif(F.col("DEATH_DATE"), F.lit(""))).alias("death_date"),
        )
    )

    zc_enc = read("ZC_DISP_ENC_TYPE").withColumnRenamed("NAME", "enc_name")
    visit_key = F.concat_ws("|", F.col("ENC_TYPE_C"), F.coalesce(F.col("ADT_PAT_CLASS_C"), F.lit("")))
    visit_vocab = _map({f"{a}|{b}": v[0] for (a, b), v in cw.CLARITY_VISIT.items()}, visit_key)
    visit_code = _map({f"{a}|{b}": v[1] for (a, b), v in cw.CLARITY_VISIT.items()}, visit_key)
    visit = (
        enc.join(hsp, "PAT_ENC_CSN_ID", "left")
        .join(zc_enc, enc["ENC_TYPE_C"] == zc_enc["DISP_ENC_TYPE_C"], "left")
        .select(
            F.lit(site).alias("site"),
            F.col("PAT_ENC_CSN_ID").alias("visit_source_value"),
            F.col("PAT_ID").alias("person_source_value"),
            visit_vocab.alias("visit_vocab"), visit_code.alias("visit_code"),
            F.concat_ws(" / ", F.col("enc_name"), F.col("ADT_PAT_CLASS_C")).alias("visit_source_text"),
            F.to_timestamp(F.coalesce("HOSP_ADMSN_TIME", "CHECKIN_TIME")).alias("start_dt"),
            F.to_timestamp(F.coalesce("HOSP_DISCH_TIME", "CHECKOUT_TIME")).alias("end_dt"),
            F.col("VISIT_PROV_ID").alias("provider_source_value"),
            F.col("DEPARTMENT_ID").alias("care_site_source_value"),
        )
    )

    enc_keys = enc.select("PAT_ENC_CSN_ID", "PAT_ID", "CONTACT_DATE", "VISIT_PROV_ID")

    # Diagnoses: PAT_ENC_DX -> CLARITY_EDG -> current ICD-10-CM code
    edg = read("CLARITY_EDG").join(read("EDG_CURRENT_ICD10").filter("LINE = '1'").drop("LINE"), "DX_ID")
    dx = (
        read("PAT_ENC_DX").join(enc_keys, "PAT_ENC_CSN_ID").join(edg, "DX_ID", "left")
        .select(
            F.lit(site).alias("site"),
            F.concat_ws("-", F.lit("dx"), "PAT_ENC_CSN_ID", "LINE").alias("event_source_id"),
            F.col("PAT_ID").alias("person_source_value"), F.col("PAT_ENC_CSN_ID").alias("visit_source_value"),
            F.col("VISIT_PROV_ID").alias("provider_source_value"), F.lit("diagnosis").alias("event_kind"),
            F.to_timestamp("CONTACT_DATE").alias("start_dt"),
            F.lit("ICD10CM").alias("vocabulary_id"), F.col("CODE").alias("code"),
            F.col("DX_NAME").alias("source_description"),
        )
    )

    # Procedures (every order, including lab orders: their CPT codes map to the Measurement domain)
    eap = read("CLARITY_EAP")
    proc = (
        read("ORDER_PROC").join(enc_keys.drop("PAT_ID"), "PAT_ENC_CSN_ID").join(eap, "PROC_ID", "left")
        .select(
            F.lit(site).alias("site"), F.concat_ws("-", F.lit("op"), "ORDER_PROC_ID").alias("event_source_id"),
            F.col("PAT_ID").alias("person_source_value"), F.col("PAT_ENC_CSN_ID").alias("visit_source_value"),
            F.col("VISIT_PROV_ID").alias("provider_source_value"), F.lit("procedure").alias("event_kind"),
            F.to_timestamp("ORDERING_DATE").alias("start_dt"), F.lit("CPT4").alias("vocabulary_id"),
            F.col("PROC_CODE").alias("code"), F.col("PROC_NAME").alias("source_description"),
        )
    )

    # Lab results: LOINC when the component has one, otherwise the local component id
    comp = read("CLARITY_COMPONENT")
    has_loinc = F.coalesce(F.col("DEFAULT_LNC_CODE"), F.lit("")) != ""
    num = F.col("ORD_NUM_VALUE").cast("double")
    not_numeric = num == F.lit(9999999.0)
    parsed = F.regexp_extract("ORD_VALUE", r"^\s*([<>])\s*([0-9.]+)\s*$", 2)
    labs = (
        read("ORDER_RESULTS").join(read("ORDER_PROC").select("ORDER_PROC_ID", "PAT_ENC_CSN_ID"), "ORDER_PROC_ID")
        .join(enc_keys, "PAT_ENC_CSN_ID").join(comp, "COMPONENT_ID", "left")
        .select(
            F.lit(site).alias("site"),
            F.concat_ws("-", F.lit("res"), "ORDER_PROC_ID", "LINE").alias("event_source_id"),
            F.col("PAT_ID").alias("person_source_value"), F.col("PAT_ENC_CSN_ID").alias("visit_source_value"),
            F.col("VISIT_PROV_ID").alias("provider_source_value"), F.lit("lab").alias("event_kind"),
            F.to_timestamp("RESULT_TIME").alias("start_dt"),
            F.when(has_loinc, "LOINC").otherwise(f"{site}_LAB_COMPONENT").alias("vocabulary_id"),
            F.when(has_loinc, F.col("DEFAULT_LNC_CODE")).otherwise(F.col("COMPONENT_ID")).alias("code"),
            F.col("NAME").alias("source_description"),
            # 9999999 means "not a plain number": recover "<x" / ">x", otherwise leave the value empty
            F.when(~not_numeric, num).when(parsed != "", parsed.cast("double")).alias("value_as_number"),
            F.col("ORD_VALUE").alias("value_source_value"),
            F.when(not_numeric, F.nullif(F.regexp_extract("ORD_VALUE", r"^\s*([<>])", 1), F.lit("")))
            .alias("operator"),
            F.col("REFERENCE_UNIT").alias("unit_code"),
        )
    )

    # Flowsheet vitals: split "128/82", keep the raw unit (oz / in) for the harmonisation step
    flo = read("IP_FLWSHT_MEAS").join(enc_keys.select("PAT_ENC_CSN_ID", "VISIT_PROV_ID"), "PAT_ENC_CSN_ID")
    vital_frames = []
    for flo_id, targets in cw.CLARITY_FLOWSHEET.items():
        for i, (loinc, unit) in enumerate(targets):
            raw = F.split("MEAS_VALUE", "/").getItem(i) if len(targets) > 1 else F.col("MEAS_VALUE")
            vital_frames.append(
                flo.filter(F.col("FLO_MEAS_ID") == flo_id).select(
                    F.lit(site).alias("site"),
                    F.concat_ws("-", F.lit("flo"), "FSD_ID", F.lit(str(i))).alias("event_source_id"),
                    F.col("PAT_ID").alias("person_source_value"),
                    F.col("PAT_ENC_CSN_ID").alias("visit_source_value"),
                    F.col("VISIT_PROV_ID").alias("provider_source_value"), F.lit("vital").alias("event_kind"),
                    F.to_timestamp("RECORDED_TIME").alias("start_dt"), F.lit("LOINC").alias("vocabulary_id"),
                    F.lit(loinc).alias("code"), F.concat(F.lit("flowsheet "), F.col("FLO_MEAS_ID")).alias(
                        "source_description"),
                    raw.cast("double").alias("value_as_number"), F.col("MEAS_VALUE").alias("value_source_value"),
                    F.lit(unit).alias("unit_code"),
                )
            )

    # Medication orders: RxNorm when the medication record has one, otherwise the local medication id
    rx = read("RXNORM_CODES").filter("LINE = '1'").select("MEDICATION_ID", "RXNORM_CODE")
    meds = (
        read("ORDER_MED").join(enc_keys.select("PAT_ENC_CSN_ID", "VISIT_PROV_ID"), "PAT_ENC_CSN_ID", "left")
        .join(rx, "MEDICATION_ID", "left").join(read("CLARITY_MEDICATION"), "MEDICATION_ID", "left")
        .select(
            F.lit(site).alias("site"), F.concat_ws("-", F.lit("om"), "ORDER_MED_ID").alias("event_source_id"),
            F.col("PAT_ID").alias("person_source_value"), F.col("PAT_ENC_CSN_ID").alias("visit_source_value"),
            F.col("VISIT_PROV_ID").alias("provider_source_value"), F.lit("drug").alias("event_kind"),
            F.to_timestamp("START_DATE").alias("start_dt"), F.to_date("END_DATE").alias("end_date"),
            F.when(F.col("RXNORM_CODE").isNotNull(), "RxNorm").otherwise(f"{site}_MEDICATION").alias("vocabulary_id"),
            F.coalesce("RXNORM_CODE", "MEDICATION_ID").alias("code"), F.col("NAME").alias("source_description"),
            F.col("QUANTITY").alias("quantity"),
            F.datediff(F.to_date("END_DATE"), F.to_date("START_DATE")).alias("days_supply"),
            F.col("REFILLS").alias("refills"),
        )
    )

    events = _events(dx)
    for frame in [proc, labs, *vital_frames, meds]:
        events = events.unionByName(_events(frame))

    care_site = read("CLARITY_DEP").select(
        F.lit(site).alias("site"), F.col("DEPARTMENT_ID").alias("care_site_source_value"),
        F.col("DEPARTMENT_NAME").alias("care_site_name"))
    provider = read("CLARITY_SER").select(
        F.lit(site).alias("site"), F.col("PROV_ID").alias("provider_source_value"),
        F.col("PROV_NAME").alias("provider_name"), F.col("SPECIALTY").alias("specialty_source_value"))
    return {"stg_person": person, "stg_visit": visit, "stg_event": events, "stg_care_site": care_site,
            "stg_provider": provider}


# =============================================================================================
# Riverside Community Health: FHIR R4 Bulk Data export (NDJSON)
# =============================================================================================
_CODING = "array<struct<system:string,code:string,display:string>>"
_CC = f"struct<coding:{_CODING},text:string>"
_QTY = "struct<value:double,unit:string,system:string,code:string>"
_REF = "struct<reference:string>"
FHIR_SCHEMAS = {
    "Patient": "id string, gender string, birthDate string, deceasedDateTime string, "
               "address array<struct<postalCode:string,state:string>>, "
               "extension array<struct<url:string,extension:array<struct<url:string,"
               "valueCoding:struct<system:string,code:string,display:string>,valueString:string>>>>",
    "Encounter": f"id string, class struct<code:string,display:string>, subject {_REF}, "
                 f"participant array<struct<individual:{_REF}>>, period struct<start:string,end:string>",
    "Condition": f"id string, code {_CC}, subject {_REF}, encounter {_REF}, onsetDateTime string, "
                 "recordedDate string",
    "Observation": f"id string, category array<struct<coding:{_CODING}>>, code {_CC}, subject {_REF}, "
                   f"encounter {_REF}, effectiveDateTime string, valueQuantity {_QTY}, "
                   f"component array<struct<code:{_CC},valueQuantity:{_QTY}>>",
    "Procedure": f"id string, code {_CC}, subject {_REF}, encounter {_REF}, performedDateTime string",
    "MedicationRequest": f"id string, medicationCodeableConcept {_CC}, subject {_REF}, encounter {_REF}, "
                         "authoredOn string, dispenseRequest struct<quantity:struct<value:double>,"
                         "expectedSupplyDuration:struct<value:int,unit:string>,numberOfRepeatsAllowed:int>",
    "Practitioner": "id string, name array<struct<family:string,given:array<string>>>",
}


def _ref_id(col: str) -> Column:
    """'Patient/rch-pat-000001' -> 'rch-pat-000001'"""
    return F.regexp_extract(F.col(col), r"/(.+)$", 1)


def _vocab_of(system: Column) -> Column:
    return _map(cw.FHIR_SYSTEM_TO_VOCAB, system)


def stage_fhir(spark: SparkSession, src: Path, site: str = "RCH") -> dict[str, DataFrame]:
    def read(rtype: str) -> DataFrame:
        # Bulk exports may repeat a resource: keep one row per resource id
        return spark.read.schema(FHIR_SCHEMAS[rtype]).json(str(src / f"{rtype}.ndjson")).dropDuplicates(["id"])

    pat = read("Patient")
    race_ext = F.filter("extension", lambda x: x["url"].endswith("us-core-race"))
    eth_ext = F.filter("extension", lambda x: x["url"].endswith("us-core-ethnicity"))
    race_code = F.element_at(race_ext, 1)["extension"][0]["valueCoding"]
    eth_code = F.element_at(eth_ext, 1)["extension"][0]["valueCoding"]
    person = pat.select(
        F.lit(site).alias("site"), F.col("id").alias("person_source_value"),
        F.to_date("birthDate").alias("birth_date"),
        _map(cw.FHIR_GENDER, F.col("gender")).alias("gender_code"), F.col("gender").alias("gender_source_value"),
        _map(cw.FHIR_RACE, race_code["code"]).alias("race_code"), race_code["display"].alias("race_source_value"),
        _map(cw.FHIR_ETHNICITY, eth_code["code"]).alias("ethnicity_code"),
        eth_code["display"].alias("ethnicity_source_value"),
        F.col("address")[0]["postalCode"].alias("zip"), F.col("address")[0]["state"].alias("state"),
        F.to_date("deceasedDateTime").alias("death_date"),
    )

    enc = read("Encounter")
    visit = enc.select(
        F.lit(site).alias("site"), F.col("id").alias("visit_source_value"),
        _ref_id("subject.reference").alias("person_source_value"),
        _map({k: v[0] for k, v in cw.FHIR_ENCOUNTER_CLASS.items()}, F.col("class.code")).alias("visit_vocab"),
        _map({k: v[1] for k, v in cw.FHIR_ENCOUNTER_CLASS.items()}, F.col("class.code")).alias("visit_code"),
        F.col("class.code").alias("visit_source_text"),
        F.to_timestamp("period.start").alias("start_dt"), F.to_timestamp("period.end").alias("end_dt"),
        F.regexp_extract(F.col("participant")[0]["individual"]["reference"], r"/(.+)$", 1)
        .alias("provider_source_value"),
        F.lit(f"{site}-MAIN").alias("care_site_source_value"),
    )
    enc_prov = visit.select(F.col("visit_source_value"), F.col("provider_source_value"))

    def common(df: DataFrame, kind: str, ts: str) -> list[Column]:
        return [F.lit(site).alias("site"), F.col("id").alias("event_source_id"),
                _ref_id("subject.reference").alias("person_source_value"),
                _ref_id("encounter.reference").alias("visit_source_value"), F.lit(kind).alias("event_kind"),
                F.to_timestamp(ts).alias("start_dt")]

    # Conditions: choose one coding per resource using the vocabulary preference order
    cond = read("Condition")
    codings = F.transform("code.coding", lambda c: F.struct(_vocab_of(c["system"]).alias("v"), c["code"].alias("c"),
                                                             c["display"].alias("d")))
    pref = F.lit(None)
    for v in reversed(cw.FHIR_CONDITION_PREFERENCE):
        pref = F.coalesce(F.element_at(F.filter(codings, lambda x, v=v: x["v"] == v), 1), pref)
    chosen = F.coalesce(pref, F.element_at(codings, 1))
    dx = cond.select(*common(cond, "diagnosis", "onsetDateTime"), chosen["v"].alias("vocabulary_id"),
                     chosen["c"].alias("code"), F.col("code.text").alias("source_description"))

    # Observations: plain value, or one row per component (blood pressure panel)
    obs = read("Observation")
    category = F.col("category")[0]["coding"][0]["code"]
    kind = F.when(category == "laboratory", "lab").otherwise("vital")
    simple = obs.filter(F.col("component").isNull()).select(
        *common(obs, "lab", "effectiveDateTime")[:4], kind.alias("event_kind"),
        F.to_timestamp("effectiveDateTime").alias("start_dt"),
        F.coalesce(_vocab_of(F.col("code.coding")[0]["system"]), F.lit("UNKNOWN")).alias("vocabulary_id"),
        F.col("code.coding")[0]["code"].alias("code"),
        F.coalesce(F.col("code.text"), F.col("code.coding")[0]["display"]).alias("source_description"),
        F.col("valueQuantity.value").alias("value_as_number"),
        F.col("valueQuantity.value").cast("string").alias("value_source_value"),
        F.col("valueQuantity.code").alias("unit_code"),
    )
    comps = obs.filter(F.col("component").isNotNull()).select(
        "id", "subject", "encounter", "effectiveDateTime", kind.alias("kind"),
        F.posexplode("component").alias("pos", "comp"))
    component = comps.select(
        F.lit(site).alias("site"), F.concat_ws("-", "id", "pos").alias("event_source_id"),
        _ref_id("subject.reference").alias("person_source_value"),
        _ref_id("encounter.reference").alias("visit_source_value"), F.col("kind").alias("event_kind"),
        F.to_timestamp("effectiveDateTime").alias("start_dt"),
        _vocab_of(F.col("comp.code.coding")[0]["system"]).alias("vocabulary_id"),
        F.col("comp.code.coding")[0]["code"].alias("code"),
        F.col("comp.code.coding")[0]["display"].alias("source_description"),
        F.col("comp.valueQuantity.value").alias("value_as_number"),
        F.col("comp.valueQuantity.value").cast("string").alias("value_source_value"),
        F.col("comp.valueQuantity.code").alias("unit_code"),
    )

    prc = read("Procedure")
    procs = prc.select(*common(prc, "procedure", "performedDateTime"),
                       _vocab_of(F.col("code.coding")[0]["system"]).alias("vocabulary_id"),
                       F.col("code.coding")[0]["code"].alias("code"),
                       F.col("code.coding")[0]["display"].alias("source_description"))

    # Medication requests: RxNorm when coded, otherwise the normalised free text becomes a local code
    mr = read("MedicationRequest")
    mcc = F.col("medicationCodeableConcept")
    has_rx = F.size(F.coalesce(mcc["coding"], F.array())) > 0
    supply = F.col("dispenseRequest.expectedSupplyDuration.value")
    meds = mr.select(
        *common(mr, "drug", "authoredOn"),
        F.date_add(F.to_date("authoredOn"), F.coalesce(supply, F.lit(30))).alias("end_date"),
        F.when(has_rx, F.lit("RxNorm")).otherwise(F.lit(f"{site}_MED_TEXT")).alias("vocabulary_id"),
        F.when(has_rx, mcc["coding"][0]["code"]).otherwise(F.lower(F.trim(mcc["text"]))).alias("code"),
        F.coalesce(mcc["text"], mcc["coding"][0]["display"]).alias("source_description"),
        F.col("dispenseRequest.quantity.value").alias("quantity"), supply.alias("days_supply"),
        F.col("dispenseRequest.numberOfRepeatsAllowed").alias("refills"),
    )

    events = _events(dx)
    for frame in [simple, component, procs, meds]:
        events = events.unionByName(_events(frame))
    # FHIR clinical resources point at the Encounter; take the clinician from there
    events = (events.drop("provider_source_value")
              .join(enc_prov, "visit_source_value", "left")
              .select(*EVENT_COLUMNS))

    care_site = spark.createDataFrame([(site, f"{site}-MAIN", "Riverside Community Health")],
                                      "site string, care_site_source_value string, care_site_name string")
    prac = read("Practitioner")
    provider = prac.select(
        F.lit(site).alias("site"), F.col("id").alias("provider_source_value"),
        F.concat_ws(" ", F.col("name")[0]["given"][0], F.col("name")[0]["family"]).alias("provider_name"),
        F.lit(None).cast("string").alias("specialty_source_value"))
    return {"stg_person": person, "stg_visit": visit, "stg_event": events, "stg_care_site": care_site,
            "stg_provider": provider}


def write_staging(frames: dict[str, DataFrame], out: Path) -> dict[str, int]:
    counts = {}
    for name, df in frames.items():
        df.write.mode("overwrite").parquet(str(out / name))
        counts[name] = df.sparkSession.read.parquet(str(out / name)).count()
    return counts
