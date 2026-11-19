"""Small, fixed crosswalks from site-specific values to OMOP (vocabulary_id, concept_code) pairs.

These are the kind of lookups an ETL specification document lists in a table. They cover short,
closed lists (sex, race, visit type, flowsheet rows) where a code-level mapping file would be overkill.
Open-ended coded data (diagnoses, labs, drugs, procedures) is never mapped here: it always goes
through the vocabulary's "Maps to" relationships or the reviewed source_to_concept_map.

Every value is a (vocabulary_id, concept_code), never a concept_id, so the same crosswalk works with
the demo vocabulary and with a real Athena download.
"""

# --- Cedar Valley Health (Epic Clarity) ---------------------------------------------------------
CLARITY_SEX = {"1": "F", "2": "M"}  # ZC_SEX.SEX_C -> Gender code
CLARITY_RACE = {"1": "5", "2": "3", "3": "2"}  # ZC_PATIENT_RACE -> Race code (99 Unknown -> no concept)
CLARITY_ETHNICITY = {"1": "Hispanic", "2": "Not Hispanic"}
# (ENC_TYPE_C, ADT_PAT_CLASS_C or "") -> visit concept
CLARITY_VISIT = {
    ("101", ""): ("Visit", "OP"),
    ("76", ""): ("CMS Place of Service", "02"),
    ("3", "1"): ("Visit", "IP"),
    ("3", "3"): ("Visit", "ER"),
}
# Flowsheet rows -> (LOINC code, unit as stored). BP is split into two measurements.
CLARITY_FLOWSHEET = {
    "5": [("8480-6", "mm[Hg]"), ("8462-4", "mm[Hg]")],
    "14": [("29463-7", "[oz_av]")],  # Epic stores weight in ounces
    "11": [("8302-2", "[in_i]")],  # and height in inches
}

# --- Riverside Community Health (FHIR R4) ------------------------------------------------------
FHIR_GENDER = {"female": "F", "male": "M"}
FHIR_RACE = {"2106-3": "5", "2054-5": "3", "2028-9": "2"}  # CDC Race & Ethnicity -> OMOP Race
FHIR_ETHNICITY = {"2135-2": "Hispanic", "2186-5": "Not Hispanic"}
FHIR_ENCOUNTER_CLASS = {
    "AMB": ("Visit", "OP"),
    "VR": ("CMS Place of Service", "02"),
    "EMER": ("Visit", "ER"),
    "IMP": ("Visit", "IP"),
}
FHIR_SYSTEM_TO_VOCAB = {
    "http://snomed.info/sct": "SNOMED",
    "http://hl7.org/fhir/sid/icd-10-cm": "ICD10CM",
    "http://loinc.org": "LOINC",
    "http://www.nlm.nih.gov/research/umls/rxnorm": "RxNorm",
    "http://www.ama-assn.org/go/cpt": "CPT4",
    "http://riverside.example.org/fhir/CodeSystem/lab": "RCH_LAB",
}
# When a Condition carries several codings, use the first vocabulary in this list. SNOMED first,
# because it is already the OMOP standard for conditions (no extra mapping hop, no loss of detail).
FHIR_CONDITION_PREFERENCE = ["SNOMED", "ICD10CM"]

# --- Shared --------------------------------------------------------------------------------------
TYPE_EHR = ("Type Concept", "OMOP4976890")
TYPE_PERIOD = ("Type Concept", "OMOP4976903")

# Unit harmonisation: (UCUM unit as received, source code it applies to or "*")
#                     -> (UCUM unit stored, multiplier, offset)
UNIT_CONVERSIONS = {
    ("[oz_av]", "*"): ("kg", 0.028349523125, 0.0),
    ("[lb_av]", "*"): ("kg", 0.45359237, 0.0),
    ("[in_i]", "*"): ("cm", 2.54, 0.0),
    # HbA1c IFCC (mmol/mol) -> NGSP (%): NGSP = 0.09148 * IFCC + 2.152 (the IFCC-NGSP master equation).
    # Only for HbA1c: other analytes reported in mmol/mol must not be touched.
    ("mmol/mol", "59261-8"): ("%", 0.09148, 2.152),
}

# Default OMOP domain for a record whose code could not be mapped (concept_id 0)
DEFAULT_DOMAIN = {"diagnosis": "Condition", "procedure": "Procedure", "lab": "Measurement",
                  "vital": "Measurement", "drug": "Drug"}
