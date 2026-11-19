"""A small demo vocabulary in the exact file format of an OHDSI Athena download.

Why a demo vocabulary?
    Real OMOP vocabularies come from https://athena.ohdsi.org (free account; some vocabularies such
    as CPT4 need a UMLS licence) and may not be redistributed in a public repo. So the project ships
    a tiny stand-in with the same tables and columns: CONCEPT, CONCEPT_RELATIONSHIP, CONCEPT_ANCESTOR,
    VOCABULARY and DOMAIN, tab-delimited with YYYYMMDD dates, exactly like Athena.

What is real and what is not
    * Every concept_code below is a real public code (ICD-10-CM, SNOMED CT, LOINC, RxNorm, CPT, UCUM)
      with a short paraphrased name.
    * The concept_id numbers are NOT Athena's. They are assigned from 2,000,000,001 upward, the range
      OMOP reserves for local (site-defined) concepts, so nobody can mistake them for real ids.
    * The ETL never hard-codes a concept_id. It always looks concepts up by (vocabulary_id,
      concept_code) and follows "Maps to" relationships, so pointing it at a real Athena download
      (`omop-etl vocab --athena <folder>`) works without code changes.
"""

from __future__ import annotations

import csv
from pathlib import Path

LOCAL_ID_START = 2_000_000_001
VALID_START, VALID_END = "19700101", "20991231"

# --- Standard concepts -------------------------------------------------------------------------
# (key, vocabulary_id, concept_code, concept_name, domain_id, concept_class_id)
STANDARD: list[tuple[str, str, str, str, str, str]] = [
    # Demographics and visit types
    ("GENDER_F", "Gender", "F", "FEMALE", "Gender", "Gender"),
    ("GENDER_M", "Gender", "M", "MALE", "Gender", "Gender"),
    ("RACE_WHITE", "Race", "5", "White", "Race", "Race"),
    ("RACE_BLACK", "Race", "3", "Black or African American", "Race", "Race"),
    ("RACE_ASIAN", "Race", "2", "Asian", "Race", "Race"),
    ("ETH_HISP", "Ethnicity", "Hispanic", "Hispanic or Latino", "Ethnicity", "Ethnicity"),
    ("ETH_NONHISP", "Ethnicity", "Not Hispanic", "Not Hispanic or Latino", "Ethnicity", "Ethnicity"),
    ("VISIT_IP", "Visit", "IP", "Inpatient Visit", "Visit", "Visit"),
    ("VISIT_OP", "Visit", "OP", "Outpatient Visit", "Visit", "Visit"),
    ("VISIT_ER", "Visit", "ER", "Emergency Room Visit", "Visit", "Visit"),
    ("VISIT_TELE", "CMS Place of Service", "02", "Telehealth", "Visit", "Visit"),
    ("TYPE_EHR", "Type Concept", "OMOP4976890", "EHR", "Type Concept", "Type Concept"),
    ("TYPE_PERIOD", "Type Concept", "OMOP4976903", "Period inferred by algorithm", "Type Concept",
     "Type Concept"),
    ("OP_LT", "Meas Value Operator", "<", "<", "Meas Value Operator", "Meas Value Operator"),
    ("OP_GT", "Meas Value Operator", ">", ">", "Meas Value Operator", "Meas Value Operator"),
    # Conditions (SNOMED CT)
    ("DM", "SNOMED", "73211009", "Diabetes mellitus", "Condition", "Clinical Finding"),
    ("T2DM", "SNOMED", "44054006", "Type 2 diabetes mellitus", "Condition", "Clinical Finding"),
    ("HYPOGLY", "SNOMED", "302866003", "Hypoglycemia", "Condition", "Clinical Finding"),
    ("HTN", "SNOMED", "59621000", "Essential hypertension", "Condition", "Clinical Finding"),
    ("HLD", "SNOMED", "55822004", "Hyperlipidemia", "Condition", "Clinical Finding"),
    ("CKD3", "SNOMED", "433144002", "Chronic kidney disease stage 3", "Condition", "Clinical Finding"),
    ("ASTHMA", "SNOMED", "195967001", "Asthma", "Condition", "Clinical Finding"),
    ("MDD", "SNOMED", "36923009", "Major depression, single episode", "Condition", "Clinical Finding"),
    ("OBESITY", "SNOMED", "414916001", "Obesity", "Condition", "Clinical Finding"),
    ("AMI", "SNOMED", "57054005", "Acute myocardial infarction", "Condition", "Clinical Finding"),
    ("HF", "SNOMED", "84114007", "Heart failure", "Condition", "Clinical Finding"),
    ("BPH", "SNOMED", "266569009", "Benign prostatic hyperplasia", "Condition", "Clinical Finding"),
    ("PREG", "SNOMED", "72892002", "Normal pregnancy", "Condition", "Clinical Finding"),
    # Observation domain (history-type facts, not diseases)
    ("EXSMOKER", "SNOMED", "8517006", "Ex-smoker", "Observation", "Context-dependent"),
    # Measurements (LOINC)
    ("A1C", "LOINC", "4548-4", "Hemoglobin A1c/Hemoglobin.total in Blood", "Measurement", "Lab Test"),
    ("A1C_HPLC", "LOINC", "17856-6", "Hemoglobin A1c/Hemoglobin.total in Blood by HPLC", "Measurement",
     "Lab Test"),
    ("A1C_IFCC", "LOINC", "59261-8", "Hemoglobin A1c/Hemoglobin.total in Blood by IFCC protocol",
     "Measurement", "Lab Test"),
    ("GLU", "LOINC", "2345-7", "Glucose [Mass/volume] in Serum or Plasma", "Measurement", "Lab Test"),
    ("GLU_BLD", "LOINC", "2339-0", "Glucose [Mass/volume] in Blood", "Measurement", "Lab Test"),
    ("CREAT", "LOINC", "2160-0", "Creatinine [Mass/volume] in Serum or Plasma", "Measurement", "Lab Test"),
    ("EGFR", "LOINC", "62238-1", "Glomerular filtration rate/1.73 sq M.predicted [CKD-EPI]",
     "Measurement", "Lab Test"),
    ("EGFR_2021", "LOINC", "98979-8", "Glomerular filtration rate/1.73 sq M.predicted by Creatinine-based "
     "formula (CKD-EPI 2021)", "Measurement", "Lab Test"),
    ("GLU_CAP", "LOINC", "41653-7", "Glucose [Mass/volume] in Capillary blood by Glucometer", "Measurement",
     "Lab Test"),
    ("EGFR_MDRD", "LOINC", "33914-3", "Glomerular filtration rate/1.73 sq M.predicted [MDRD]",
     "Measurement", "Lab Test"),
    ("LDL", "LOINC", "13457-7", "Cholesterol in LDL [Mass/volume] in Serum or Plasma by calculation",
     "Measurement", "Lab Test"),
    ("LDL_DIRECT", "LOINC", "18262-6", "Cholesterol in LDL [Mass/volume] in Serum or Plasma by Direct assay",
     "Measurement", "Lab Test"),
    ("SBP", "LOINC", "8480-6", "Systolic blood pressure", "Measurement", "Clinical Observation"),
    ("DBP", "LOINC", "8462-4", "Diastolic blood pressure", "Measurement", "Clinical Observation"),
    ("WT", "LOINC", "29463-7", "Body weight", "Measurement", "Clinical Observation"),
    ("HT", "LOINC", "8302-2", "Body height", "Measurement", "Clinical Observation"),
    ("BMI", "LOINC", "39156-5", "Body mass index (BMI) [Ratio]", "Measurement", "Clinical Observation"),
    # Units (UCUM)
    ("U_PCT", "UCUM", "%", "percent", "Unit", "Unit"),
    ("U_MGDL", "UCUM", "mg/dL", "milligram per deciliter", "Unit", "Unit"),
    ("U_EGFR", "UCUM", "mL/min/{1.73_m2}", "milliliter per minute per 1.73 square meter", "Unit", "Unit"),
    ("U_MMHG", "UCUM", "mm[Hg]", "millimeter mercury column", "Unit", "Unit"),
    ("U_KG", "UCUM", "kg", "kilogram", "Unit", "Unit"),
    ("U_CM", "UCUM", "cm", "centimeter", "Unit", "Unit"),
    ("U_KGM2", "UCUM", "kg/m2", "kilogram per square meter", "Unit", "Unit"),
    # Drugs (RxNorm): ingredients and the clinical drugs (SCDs) that contain them
    ("METFORMIN", "RxNorm", "6809", "metformin", "Drug", "Ingredient"),
    ("GLIPIZIDE", "RxNorm", "4821", "glipizide", "Drug", "Ingredient"),
    ("LISINOPRIL", "RxNorm", "29046", "lisinopril", "Drug", "Ingredient"),
    ("ATORVASTATIN", "RxNorm", "83367", "atorvastatin", "Drug", "Ingredient"),
    ("GLARGINE", "RxNorm", "274783", "insulin glargine", "Drug", "Ingredient"),
    ("SERTRALINE", "RxNorm", "36437", "sertraline", "Drug", "Ingredient"),
    ("METFORMIN_500", "RxNorm", "861007", "metformin hydrochloride 500 MG Oral Tablet", "Drug",
     "Clinical Drug"),
    ("METFORMIN_ER_500", "RxNorm", "860975", "24 HR metformin hydrochloride 500 MG Extended Release Oral Tablet",
     "Drug", "Clinical Drug"),
    ("GLIPIZIDE_5", "RxNorm", "310490", "glipizide 5 MG Oral Tablet", "Drug", "Clinical Drug"),
    ("LISINOPRIL_10", "RxNorm", "314076", "lisinopril 10 MG Oral Tablet", "Drug", "Clinical Drug"),
    ("ATORVASTATIN_20", "RxNorm", "617310", "atorvastatin 20 MG Oral Tablet", "Drug", "Clinical Drug"),
    ("GLARGINE_100", "RxNorm", "261551", "insulin glargine 100 UNT/ML Injectable Solution", "Drug",
     "Clinical Drug"),
    ("SERTRALINE_50", "RxNorm", "312940", "sertraline 50 MG Oral Tablet", "Drug", "Clinical Drug"),
    # Procedures (CPT4). Lab-test CPT codes live in the Measurement domain in OMOP.
    ("CPT_ECG", "CPT4", "93000", "Electrocardiogram, routine, with interpretation and report", "Procedure",
     "CPT4"),
    ("CPT_VENI", "CPT4", "36415", "Collection of venous blood by venipuncture", "Procedure", "CPT4"),
    ("CPT_A1C", "CPT4", "83036", "Hemoglobin; glycosylated (A1C)", "Measurement", "CPT4"),
    ("CPT_LIPID", "CPT4", "80061", "Lipid panel", "Measurement", "CPT4"),
    ("CPT_BMP", "CPT4", "80048", "Basic metabolic panel (calcium, total)", "Measurement", "CPT4"),
    ("CPT_DSME", "CPT4", "97802", "Medical nutrition therapy; initial assessment", "Procedure", "CPT4"),
]

# --- Source (non-standard) concepts and their "Maps to" targets ----------------------------------
# (vocabulary_id, concept_code, concept_name, domain_id, concept_class_id, [standard keys], invalid)
SOURCE: list[tuple[str, str, str, str, str, list[str], str]] = [
    ("ICD10CM", "E11.9", "Type 2 diabetes mellitus without complications", "Condition", "5-char billing code",
     ["T2DM"], ""),
    # One source code can map to TWO standard concepts: diabetes AND hypoglycemia.
    ("ICD10CM", "E11.649", "Type 2 diabetes mellitus with hypoglycemia without coma", "Condition",
     "7-char billing code", ["T2DM", "HYPOGLY"], ""),
    ("ICD10CM", "E16.2", "Hypoglycemia, unspecified", "Condition", "4-char billing code", ["HYPOGLY"], ""),
    ("ICD10CM", "I10", "Essential (primary) hypertension", "Condition", "3-char billing code", ["HTN"], ""),
    ("ICD10CM", "E78.5", "Hyperlipidemia, unspecified", "Condition", "4-char billing code", ["HLD"], ""),
    ("ICD10CM", "N18.30", "Chronic kidney disease, stage 3 unspecified", "Condition", "5-char billing code",
     ["CKD3"], ""),
    # N18.3 was retired on 2020-10-01 (split into N18.30/31/32). Old records still use it.
    ("ICD10CM", "N18.3", "Chronic kidney disease, stage 3 (moderate)", "Condition", "4-char billing code",
     ["CKD3"], "D"),
    ("ICD10CM", "J45.909", "Unspecified asthma, uncomplicated", "Condition", "6-char billing code",
     ["ASTHMA"], ""),
    ("ICD10CM", "F32.9", "Major depressive disorder, single episode, unspecified", "Condition",
     "4-char billing code", ["MDD"], ""),
    ("ICD10CM", "E66.9", "Obesity, unspecified", "Condition", "4-char billing code", ["OBESITY"], ""),
    ("ICD10CM", "I21.9", "Acute myocardial infarction, unspecified", "Condition", "4-char billing code",
     ["AMI"], ""),
    ("ICD10CM", "I50.9", "Heart failure, unspecified", "Condition", "4-char billing code", ["HF"], ""),
    ("ICD10CM", "N40.0", "Benign prostatic hyperplasia without lower urinary tract symptoms", "Condition",
     "4-char billing code", ["BPH"], ""),
    ("ICD10CM", "Z34.90", "Encounter for supervision of normal pregnancy, unspecified trimester",
     "Condition", "6-char billing code", ["PREG"], ""),
    # A Z-code whose standard target is in the Observation domain, so the row lands in OBSERVATION.
    ("ICD10CM", "Z87.891", "Personal history of nicotine dependence", "Observation", "6-char billing code",
     ["EXSMOKER"], ""),
]

# Concept hierarchy rows (ancestor key, descendant key, levels apart). Self rows are added automatically.
ANCESTRY: list[tuple[str, str, int]] = [
    ("DM", "T2DM", 1),
    ("METFORMIN", "METFORMIN_500", 1),
    ("METFORMIN", "METFORMIN_ER_500", 1),
    ("GLIPIZIDE", "GLIPIZIDE_5", 1),
    ("LISINOPRIL", "LISINOPRIL_10", 1),
    ("ATORVASTATIN", "ATORVASTATIN_20", 1),
    ("GLARGINE", "GLARGINE_100", 1),
    ("SERTRALINE", "SERTRALINE_50", 1),
]

VOCABULARIES = {
    "None": "OMOP Standardized Vocabularies", "Gender": "OMOP Gender", "Race": "Race and Ethnicity Code Set (USBC)", "Ethnicity": "OMOP Ethnicity",
    "Visit": "OMOP Visit", "CMS Place of Service": "Place of Service Codes for Professional Claims (CMS)",
    "Type Concept": "OMOP Type Concept", "Meas Value Operator": "OMOP Measurement Value Operator",
    "SNOMED": "Systematic Nomenclature of Medicine - Clinical Terms", "LOINC": "Logical Observation "
    "Identifiers Names and Codes", "UCUM": "Unified Code for Units of Measure", "RxNorm": "RxNorm (NLM)",
    "CPT4": "Current Procedural Terminology version 4 (AMA)",
    "ICD10CM": "International Classification of Diseases, Tenth Revision, Clinical Modification (NCHS)",
}
DOMAINS = ["Metadata", "Gender", "Race", "Ethnicity", "Visit", "Type Concept", "Meas Value Operator", "Condition",
           "Observation", "Measurement", "Unit", "Drug", "Procedure"]

CONCEPT_COLS = ["concept_id", "concept_name", "domain_id", "vocabulary_id", "concept_class_id",
                "standard_concept", "concept_code", "valid_start_date", "valid_end_date", "invalid_reason"]


def build_rows() -> dict[str, list[dict]]:
    """Return every vocabulary table as a list of row dicts, with deterministic local concept ids."""
    # Athena always ships concept 0: the target for any record that could not be mapped.
    concept: list[dict] = [dict(zip(CONCEPT_COLS, [0, "No matching concept", "Metadata", "None", "Undefined", "",
                                                   "No matching concept", VALID_START, VALID_END, ""], strict=True))]
    ids: dict[str, int] = {}
    next_id = LOCAL_ID_START

    for key, vocab, code, name, domain, cls in STANDARD:
        ids[key] = next_id
        concept.append(dict(zip(CONCEPT_COLS, [next_id, name, domain, vocab, cls, "S", code,
                                               VALID_START, VALID_END, ""], strict=True)))
        next_id += 1

    relationship: list[dict] = []
    for vocab, code, name, domain, cls, targets, invalid in SOURCE:
        cid = next_id
        next_id += 1
        end = "20200930" if invalid else VALID_END
        concept.append(dict(zip(CONCEPT_COLS, [cid, name, domain, vocab, cls, "", code, VALID_START, end,
                                               invalid], strict=True)))
        for t in targets:
            relationship.append({"concept_id_1": cid, "concept_id_2": ids[t], "relationship_id": "Maps to",
                                 "valid_start_date": VALID_START, "valid_end_date": VALID_END,
                                 "invalid_reason": ""})
            relationship.append({"concept_id_1": ids[t], "concept_id_2": cid, "relationship_id": "Mapped from",
                                 "valid_start_date": VALID_START, "valid_end_date": VALID_END,
                                 "invalid_reason": ""})

    # Standard concepts map to themselves; Athena ships these rows too.
    for cid in ids.values():
        relationship.append({"concept_id_1": cid, "concept_id_2": cid, "relationship_id": "Maps to",
                             "valid_start_date": VALID_START, "valid_end_date": VALID_END,
                             "invalid_reason": ""})

    ancestor = [{"ancestor_concept_id": cid, "descendant_concept_id": cid, "min_levels_of_separation": 0,
                 "max_levels_of_separation": 0} for cid in ids.values()]
    for a, d, lvl in ANCESTRY:
        ancestor.append({"ancestor_concept_id": ids[a], "descendant_concept_id": ids[d],
                         "min_levels_of_separation": lvl, "max_levels_of_separation": lvl})

    vocabulary = [{"vocabulary_id": v, "vocabulary_name": n, "vocabulary_reference": "demo",
                   "vocabulary_version": "demo-2026-11", "vocabulary_concept_id": 0}
                  for v, n in VOCABULARIES.items()]
    domain = [{"domain_id": d, "domain_name": d, "domain_concept_id": 0} for d in DOMAINS]
    return {"CONCEPT": concept, "CONCEPT_RELATIONSHIP": relationship, "CONCEPT_ANCESTOR": ancestor,
            "VOCABULARY": vocabulary, "DOMAIN": domain}


def write_demo_vocabulary(out_dir: Path) -> dict[str, int]:
    """Write Athena-format CSVs (tab-delimited, with header). Returns row counts per table."""
    out_dir.mkdir(parents=True, exist_ok=True)
    counts = {}
    for table, rows in build_rows().items():
        with open(out_dir / f"{table}.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0]), delimiter="\t")
            w.writeheader()
            w.writerows(rows)
        counts[table] = len(rows)
    (out_dir / "SOURCE.txt").write_text("demo\n")
    return counts
