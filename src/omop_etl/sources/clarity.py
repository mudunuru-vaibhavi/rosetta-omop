"""Cedar Valley Health (fictional) — writes the simulation as an Epic Clarity-style extract.

Clarity is Epic's reporting database. Health systems usually share it as pipe-delimited table dumps.
The table and column names below follow common Clarity naming (PATIENT, PAT_ENC, PAT_ENC_DX,
ORDER_PROC, ORDER_RESULTS, ORDER_MED, IP_FLWSHT_MEAS, CLARITY_EDG, ...), trimmed to what an OMOP ETL
needs. Several real-world Clarity habits are reproduced on purpose because the ETL must handle them:

* Category lists ("_C" columns) are numbers that must be decoded through ZC_ tables.
* Vitals are flowsheet rows: blood pressure is one string ("128/82"), weight is stored in OUNCES and
  height in INCHES.
* Lab results carry ORD_VALUE (text) and ORD_NUM_VALUE, which holds 9999999 when the result is not
  a plain number (">14.0", "HEMOLYZED").
* Some lab components have no LOINC code and one formulary medication has no RxNorm code: they
  arrive unmapped and are handled by the source_to_concept_map review loop (mapping_assist/).
* Some CKD diagnoses still use ICD-10-CM N18.3, a code retired in October 2020.
* Two patients have a birth year typed 100 years late (data-entry error) for the quality checks to find.
"""

from __future__ import annotations

import csv
import hashlib
import json
from collections import defaultdict
from datetime import timedelta
from pathlib import Path

import numpy as np

from .clinical_model import Patient, simulate_site, write_truth

SITE = "CVH"

ZC_SEX = {1: "Female", 2: "Male"}
ZC_PATIENT_RACE = {1: "White", 2: "Black or African American", 3: "Asian", 99: "Unknown"}
ZC_ETHNIC_GROUP = {1: "Hispanic or Latino", 2: "Not Hispanic or Latino"}
ZC_DISP_ENC_TYPE = {101: "Office Visit", 76: "Telemedicine", 3: "Hospital Encounter"}
ZC_PAT_CLASS = {1: "Inpatient", 3: "Emergency"}
RACE_TO_C = {"5": 1, "3": 2, "2": 3, "UNK": 99}

DEPARTMENTS = {
    "PRIMARY CARE": (101001, "CVH NORTHGATE FAMILY MEDICINE", "Family Medicine"),
    "EMERGENCY": (101002, "CVMC EMERGENCY", "Emergency Medicine"),
    "INPATIENT MEDICINE": (101003, "CVMC INPATIENT MEDICINE", "Hospital Medicine"),
}

# DX_ID -> (name, current ICD-10-CM code). Neutral condition key -> DX_ID.
EDG = {
    300001: ("Type 2 diabetes mellitus without complication", "E11.9"),
    300002: ("Type 2 diabetes mellitus with hypoglycemia without coma", "E11.649"),
    300003: ("Essential hypertension", "I10"),
    300004: ("Hyperlipidemia", "E78.5"),
    300005: ("CKD stage 3", "N18.30"),
    300006: ("CKD stage 3 (legacy)", "N18.3"),
    300007: ("Asthma", "J45.909"),
    300008: ("Depression, major, single episode", "F32.9"),
    300009: ("Obesity", "E66.9"),
    300010: ("Acute myocardial infarction", "I21.9"),
    300011: ("BPH", "N40.0"),
    300012: ("Normal pregnancy", "Z34.90"),
    300013: ("History of tobacco use", "Z87.891"),
}
DX_FOR = {"T2DM": 300001, "HYPO_DM": 300002, "HTN": 300003, "HLD": 300004, "CKD3": 300005,
          "ASTHMA": 300007, "MDD": 300008, "OBESITY": 300009, "AMI": 300010, "BPH": 300011,
          "PREG": 300012, "EXSMOKER": 300013}

# PROC_ID -> (name, CPT code)
EAP = {
    400001: ("HEMOGLOBIN A1C", "83036"),
    400002: ("BASIC METABOLIC PANEL", "80048"),
    400003: ("LIPID PANEL", "80061"),
    400004: ("VENIPUNCTURE", "36415"),
    400005: ("ECG 12 LEAD", "93000"),
    400006: ("DIABETES NUTRITION THERAPY INITIAL", "97802"),
}
PROC_FOR = {"CPT_VENI": 400004, "CPT_ECG": 400005, "CPT_DSME": 400006}

# COMPONENT_ID -> (name, LOINC or "" when the lab never assigned one)
COMPONENTS = {
    1510: ("HEMOGLOBIN A1C", "4548-4"),
    1511: ("POC HEMOGLOBIN A1C", ""),
    1520: ("GLUCOSE", "2345-7"),
    1530: ("CREATININE", "2160-0"),
    1540: ("EGFR (CKD-EPI)", "62238-1"),
    1541: ("EGFR CKD-EPI 2021 RACE FREE", ""),
    1550: ("LDL CHOLESTEROL CALCULATED", "13457-7"),
}

# MEDICATION_ID -> (name, RxNorm SCD or "" when missing)
MEDS = {
    500001: ("METFORMIN 500 MG TABLET", "861007"),
    500002: ("GLIPIZIDE 5 MG TABLET", "310490"),
    500003: ("LISINOPRIL 10 MG TABLET", "314076"),
    500004: ("ATORVASTATIN 20 MG TABLET", "617310"),
    500005: ("LANTUS 100 UNIT/ML SOLN", "261551"),
    500006: ("SERTRALINE 50 MG TABLET", "312940"),
    500007: ("GLIPIZIDE 5 MG TAB (CVH FORMULARY SUB)", ""),
}
MED_FOR = {"METFORMIN_500": 500001, "GLIPIZIDE_5": 500002, "LISINOPRIL_10": 500003,
           "ATORVASTATIN_20": 500004, "GLARGINE_100": 500005, "SERTRALINE_50": 500006}

FLO = {5: "BLOOD PRESSURE", 14: "WEIGHT/SCALE", 11: "HEIGHT"}


def _write(out: Path, table: str, header: list[str], rows: list[list]) -> dict:
    path = out / f"{table}.txt"
    with open(path, "w", newline="") as f:
        w = csv.writer(f, delimiter="|", lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)
    return {"file": path.name, "rows": len(rows), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def write_clarity(patients: list[Patient], out: Path, seed: int = 11) -> dict:
    """Serialise patients as Clarity tables in `out`. Returns the manifest."""
    rng = np.random.default_rng(seed)
    out.mkdir(parents=True, exist_ok=True)
    t: dict[str, list[list]] = defaultdict(list)

    typo = set(rng.choice(len(patients), size=min(2, len(patients)), replace=False).tolist())
    order_proc_id, order_med_id, fsd = 9_000_000, 7_000_000, 0

    for p in patients:
        pat_id = f"Z{100000 + p.idx}"
        birth = p.birth.replace(year=p.birth.year + 100) if p.idx in typo else p.birth
        t["PATIENT"].append([pat_id, f"CVH{2000000 + p.idx}", birth.isoformat(), 1 if p.sex == "F" else 2,
                             1 if p.ethnicity == "Hispanic" else 2, p.zip, "OH",
                             p.death.isoformat() if p.death else ""])
        t["PATIENT_RACE"].append([pat_id, 1, RACE_TO_C[p.race]])

        for e in p.encounters:
            csn = 50_000_000 + e.seq
            enc_type = {"OP": 101, "TELE": 76, "ER": 3, "IP": 3}[e.kind]
            dept = DEPARTMENTS[e.dept][0]
            t["PAT_ENC"].append([csn, pat_id, e.start.date().isoformat(), enc_type, dept, f"CVHP{e.provider:03d}",
                                 e.start.isoformat(sep=" "), e.end.isoformat(sep=" ")])
            if e.kind in ("ER", "IP"):
                t["PAT_ENC_HSP"].append([csn, 1 if e.kind == "IP" else 3, e.start.isoformat(sep=" "),
                                         e.end.isoformat(sep=" ")])
            for line, key in enumerate(e.dx, start=1):
                dx_id = DX_FOR[key]
                if key == "CKD3" and rng.random() < 0.4:
                    dx_id = 300006  # legacy code still on the problem list
                t["PAT_ENC_DX"].append([csn, line, dx_id, "Y" if line == 1 else "N"])

            # Lab orders and their result components
            groups: dict[int, list] = defaultdict(list)
            for lab in e.labs:
                proc = {"A1C": 400001, "GLU": 400002, "CREAT": 400002, "EGFR": 400002, "LDL": 400003}[lab.key]
                groups[proc].append(lab)
            for proc_id, labs in groups.items():
                order_proc_id += 1
                t["ORDER_PROC"].append([order_proc_id, csn, pat_id, proc_id, e.start.date().isoformat(), "Final"])
                for line, lab in enumerate(labs, start=1):
                    comp = {"A1C": 1510, "GLU": 1520, "CREAT": 1530, "EGFR": 1540, "LDL": 1550}[lab.key]
                    if lab.key == "A1C" and rng.random() < 0.3:
                        comp = 1511  # point-of-care analyser, no LOINC on file
                    if lab.key == "EGFR" and e.start.year >= 2024 and rng.random() < 0.5:
                        comp = 1541  # new race-free equation built without a LOINC code
                    text, num = str(lab.value), lab.value
                    if lab.key == "A1C" and rng.random() < 0.01:
                        text, num = ">14.0", 9999999
                    elif lab.key == "GLU" and rng.random() < 0.006:
                        text, num = "HEMOLYZED", 9999999
                    t["ORDER_RESULTS"].append([order_proc_id, line, comp, text, num, lab.unit,
                                               lab.time.isoformat(sep=" ")])
            for key in e.procs:
                if key in PROC_FOR:
                    order_proc_id += 1
                    t["ORDER_PROC"].append([order_proc_id, csn, pat_id, PROC_FOR[key], e.start.date().isoformat(),
                                            "Completed"])

            # Flowsheet vitals (Epic stores weight in ounces and height in inches)
            if e.vitals:
                for flo, value in ((5, f"{int(e.vitals['SBP'])}/{int(e.vitals['DBP'])}"),
                                   (14, f"{e.vitals['WT'] * 35.27396:.1f}"),
                                   (11, f"{e.vitals['HT'] / 2.54:.1f}")):
                    fsd += 1
                    t["IP_FLWSHT_MEAS"].append([fsd, pat_id, csn, flo, value, e.start.isoformat(sep=" ")])

            for f in e.fills:
                order_med_id += 1
                med = MED_FOR[f.key]
                if f.key == "GLIPIZIDE_5" and rng.random() < 0.2:
                    med = 500007
                end = f.start + timedelta(days=f.days_supply)
                t["ORDER_MED"].append([order_med_id, pat_id, csn, med, f.start.isoformat(),
                                       end.isoformat(), f.quantity, f.refills, "Sent"])

    tables = {
        "PATIENT": ["PAT_ID", "PAT_MRN_ID", "BIRTH_DATE", "SEX_C", "ETHNIC_GROUP_C", "ZIP", "STATE",
                    "DEATH_DATE"],
        "PATIENT_RACE": ["PAT_ID", "LINE", "PATIENT_RACE_C"],
        "PAT_ENC": ["PAT_ENC_CSN_ID", "PAT_ID", "CONTACT_DATE", "ENC_TYPE_C", "DEPARTMENT_ID", "VISIT_PROV_ID",
                    "CHECKIN_TIME", "CHECKOUT_TIME"],
        "PAT_ENC_HSP": ["PAT_ENC_CSN_ID", "ADT_PAT_CLASS_C", "HOSP_ADMSN_TIME", "HOSP_DISCH_TIME"],
        "PAT_ENC_DX": ["PAT_ENC_CSN_ID", "LINE", "DX_ID", "PRIMARY_DX_YN"],
        "ORDER_PROC": ["ORDER_PROC_ID", "PAT_ENC_CSN_ID", "PAT_ID", "PROC_ID", "ORDERING_DATE", "ORDER_STATUS"],
        "ORDER_RESULTS": ["ORDER_PROC_ID", "LINE", "COMPONENT_ID", "ORD_VALUE", "ORD_NUM_VALUE",
                          "REFERENCE_UNIT", "RESULT_TIME"],
        "IP_FLWSHT_MEAS": ["FSD_ID", "PAT_ID", "PAT_ENC_CSN_ID", "FLO_MEAS_ID", "MEAS_VALUE", "RECORDED_TIME"],
        "ORDER_MED": ["ORDER_MED_ID", "PAT_ID", "PAT_ENC_CSN_ID", "MEDICATION_ID", "START_DATE", "END_DATE",
                      "QUANTITY", "REFILLS", "ORDER_STATUS"],
    }
    # Reference (dimension) tables
    t["CLARITY_EDG"] = [[k, v[0]] for k, v in EDG.items()]
    t["EDG_CURRENT_ICD10"] = [[k, 1, v[1]] for k, v in EDG.items()]
    t["CLARITY_EAP"] = [[k, v[0], v[1]] for k, v in EAP.items()]
    t["CLARITY_COMPONENT"] = [[k, v[0], v[1]] for k, v in COMPONENTS.items()]
    t["CLARITY_MEDICATION"] = [[k, v[0]] for k, v in MEDS.items()]
    t["RXNORM_CODES"] = [[k, 1, v[1], "SCD"] for k, v in MEDS.items() if v[1]]
    t["CLARITY_DEP"] = [[v[0], v[1], v[2]] for v in DEPARTMENTS.values()]
    t["CLARITY_SER"] = [[f"CVHP{i:03d}", f"PROVIDER {i:03d}", "Physician" if i % 4 else "Nurse Practitioner",
                         "Family Medicine" if i % 3 else "Internal Medicine"] for i in range(24)]
    t["IP_FLO_GP_DATA"] = [[k, v] for k, v in FLO.items()]
    t["ZC_SEX"] = [[k, v] for k, v in ZC_SEX.items()]
    t["ZC_PATIENT_RACE"] = [[k, v] for k, v in ZC_PATIENT_RACE.items()]
    t["ZC_ETHNIC_GROUP"] = [[k, v] for k, v in ZC_ETHNIC_GROUP.items()]
    t["ZC_DISP_ENC_TYPE"] = [[k, v] for k, v in ZC_DISP_ENC_TYPE.items()]
    t["ZC_PAT_CLASS"] = [[k, v] for k, v in ZC_PAT_CLASS.items()]
    tables.update({
        "CLARITY_EDG": ["DX_ID", "DX_NAME"], "EDG_CURRENT_ICD10": ["DX_ID", "LINE", "CODE"],
        "CLARITY_EAP": ["PROC_ID", "PROC_NAME", "PROC_CODE"],
        "CLARITY_COMPONENT": ["COMPONENT_ID", "NAME", "DEFAULT_LNC_CODE"],
        "CLARITY_MEDICATION": ["MEDICATION_ID", "NAME"],
        "RXNORM_CODES": ["MEDICATION_ID", "LINE", "RXNORM_CODE", "RXNORM_TERM_TYPE"],
        "CLARITY_DEP": ["DEPARTMENT_ID", "DEPARTMENT_NAME", "SPECIALTY"],
        "CLARITY_SER": ["PROV_ID", "PROV_NAME", "PROV_TYPE", "SPECIALTY"],
        "IP_FLO_GP_DATA": ["FLO_MEAS_ID", "DISP_NAME"],
        "ZC_SEX": ["SEX_C", "NAME"], "ZC_PATIENT_RACE": ["PATIENT_RACE_C", "NAME"],
        "ZC_ETHNIC_GROUP": ["ETHNIC_GROUP_C", "NAME"], "ZC_DISP_ENC_TYPE": ["DISP_ENC_TYPE_C", "NAME"],
        "ZC_PAT_CLASS": ["ADT_PAT_CLASS_C", "NAME"],
    })

    files = [_write(out, name, header, t.get(name, [])) for name, header in tables.items()]
    manifest = {"site": SITE, "format": "clarity-pipe-delimited", "files": files}
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest


def generate(out: Path, n_patients: int = 6000, seed: int = 2026) -> dict:
    patients = simulate_site(SITE, n_patients, seed)
    write_truth(patients, SITE, out.parent / "_ground_truth")
    return write_clarity(patients, out)
