"""Riverside Community Health (fictional) — writes the simulation as a FHIR R4 Bulk Data export.

FHIR (Fast Healthcare Interoperability Resources) is the HL7 standard for exchanging health data as
JSON "resources" (Patient, Encounter, Condition, Observation, MedicationRequest, Procedure, ...).
A Bulk Data export ($export) delivers one NDJSON file per resource type: one JSON resource per line.

Real-world FHIR habits reproduced here on purpose:

* Conditions are coded in ICD-10-CM for some records and SNOMED CT for others (often both).
* Race and ethnicity come as US Core extensions with CDC Race & Ethnicity codes.
* Blood pressure is ONE Observation (LOINC 85354-9 panel) with two components (systolic, diastolic).
* Weight is sometimes in pounds ([lb_av]), height sometimes in inches ([in_i]) — UCUM units.
* A few HbA1c results are reported in IFCC units (mmol/mol, LOINC 59261-8) instead of NGSP percent.
* Four HbA1c results say "%" but hold mmol/mol numbers (a unit mislabel the quality checks must catch).
* Two lab tests use only a local code system (no LOINC); some prescriptions are text only.
* Bulk exports can repeat a resource; three Observations appear twice.
* Three Encounters end before they start.
"""

from __future__ import annotations

import hashlib
import json
from datetime import timedelta
from pathlib import Path

import numpy as np

from .clinical_model import Patient, simulate_site, write_truth

SITE = "RCH"
SYS_SNOMED = "http://snomed.info/sct"
SYS_ICD10 = "http://hl7.org/fhir/sid/icd-10-cm"
SYS_LOINC = "http://loinc.org"
SYS_RXNORM = "http://www.nlm.nih.gov/research/umls/rxnorm"
SYS_CPT = "http://www.ama-assn.org/go/cpt"
SYS_UCUM = "http://unitsofmeasure.org"
SYS_LOCAL_LAB = "http://riverside.example.org/fhir/CodeSystem/lab"
SYS_CDC_RACE = "urn:oid:2.16.840.1.113883.6.238"

CONDITION_CODES = {  # key -> (ICD-10-CM, SNOMED, display)
    "T2DM": ("E11.9", "44054006", "Type 2 diabetes mellitus"),
    "HYPO_DM": ("E16.2", "302866003", "Hypoglycemia"),
    "HTN": ("I10", "59621000", "Essential hypertension"),
    "HLD": ("E78.5", "55822004", "Hyperlipidemia"),
    "CKD3": ("N18.30", "433144002", "Chronic kidney disease stage 3"),
    "ASTHMA": ("J45.909", "195967001", "Asthma"),
    "MDD": ("F32.9", "36923009", "Major depression, single episode"),
    "OBESITY": ("E66.9", "414916001", "Obesity"),
    "AMI": ("I21.9", "57054005", "Acute myocardial infarction"),
    "BPH": ("N40.0", "266569009", "Benign prostatic hyperplasia"),
    "PREG": ("Z34.90", "72892002", "Normal pregnancy"),
    "EXSMOKER": ("Z87.891", "8517006", "Ex-smoker"),
}
LAB_LOINC = {"A1C": ("4548-4", "Hemoglobin A1c"), "GLU": ("2345-7", "Glucose"), "CREAT": ("2160-0", "Creatinine"),
             "EGFR": ("62238-1", "eGFR CKD-EPI"), "LDL": ("13457-7", "LDL cholesterol (calc)")}
RX = {"METFORMIN_500": ("861007", "metformin hydrochloride 500 MG Oral Tablet"),
      "GLIPIZIDE_5": ("310490", "glipizide 5 MG Oral Tablet"),
      "LISINOPRIL_10": ("314076", "lisinopril 10 MG Oral Tablet"),
      "ATORVASTATIN_20": ("617310", "atorvastatin 20 MG Oral Tablet"),
      "GLARGINE_100": ("261551", "insulin glargine 100 UNT/ML Injectable Solution"),
      "SERTRALINE_50": ("312940", "sertraline 50 MG Oral Tablet")}
CPT = {"CPT_VENI": ("36415", "Venipuncture"), "CPT_ECG": ("93000", "Electrocardiogram"),
       "CPT_DSME": ("97802", "Medical nutrition therapy, initial")}
RACE_CDC = {"5": ("2106-3", "White"), "3": ("2054-5", "Black or African American"), "2": ("2028-9", "Asian")}
ENC_CLASS = {"OP": ("AMB", "ambulatory"), "TELE": ("VR", "virtual"), "ER": ("EMER", "emergency"),
             "IP": ("IMP", "inpatient encounter")}


def _coding(system: str, code: str, display: str) -> dict:
    return {"system": system, "code": code, "display": display}


def _qty(value: float, unit: str) -> dict:
    return {"value": value, "unit": unit, "system": SYS_UCUM, "code": unit}


def write_fhir(patients: list[Patient], out: Path, seed: int = 13) -> dict:
    rng = np.random.default_rng(seed)
    out.mkdir(parents=True, exist_ok=True)
    res: dict[str, list[dict]] = {k: [] for k in ["Patient", "Encounter", "Condition", "Observation",
                                                  "MedicationRequest", "Procedure", "Practitioner"]}
    obs_n = cond_n = med_n = proc_n = 0
    a1c_mislabels = 0
    broken_periods = 0

    for i in range(24):
        res["Practitioner"].append({"resourceType": "Practitioner", "id": f"rch-prac-{i:03d}",
                                    "name": [{"family": f"Provider{i:03d}", "given": ["R"]}]})

    for p in patients:
        pid = f"rch-pat-{p.idx:06d}"
        ext = []
        if p.race in RACE_CDC:
            code, disp = RACE_CDC[p.race]
            ext.append({"url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race",
                        "extension": [{"url": "ombCategory", "valueCoding": _coding(SYS_CDC_RACE, code, disp)},
                                      {"url": "text", "valueString": disp}]})
        eth_code = ("2135-2", "Hispanic or Latino") if p.ethnicity == "Hispanic" else ("2186-5",
                                                                                       "Not Hispanic or Latino")
        ext.append({"url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity",
                    "extension": [{"url": "ombCategory", "valueCoding": _coding(SYS_CDC_RACE, *eth_code)}]})
        patient = {"resourceType": "Patient", "id": pid, "extension": ext,
                   "identifier": [{"system": "http://riverside.example.org/mrn", "value": f"R{p.idx:07d}"}],
                   "gender": "female" if p.sex == "F" else "male", "birthDate": p.birth.isoformat(),
                   "address": [{"postalCode": p.zip, "state": "OH"}]}
        if p.death:
            patient["deceasedDateTime"] = p.death.isoformat()
        res["Patient"].append(patient)

        for e in p.encounters:
            eid = f"rch-enc-{e.seq:07d}"
            cls = ENC_CLASS[e.kind]
            start, end = e.start, e.end
            if broken_periods < 3 and e.kind == "OP" and rng.random() < 0.002:
                end = start - timedelta(hours=2)
                broken_periods += 1
            res["Encounter"].append({
                "resourceType": "Encounter", "id": eid, "status": "finished",
                "class": {"system": "http://terminology.hl7.org/CodeSystem/v3-ActCode", "code": cls[0],
                          "display": cls[1]},
                "subject": {"reference": f"Patient/{pid}"},
                "participant": [{"individual": {"reference": f"Practitioner/rch-prac-{e.provider:03d}"}}],
                "period": {"start": start.isoformat(), "end": end.isoformat()},
                "serviceProvider": {"display": "Riverside Community Health"},
            })
            ref = {"subject": {"reference": f"Patient/{pid}"}, "encounter": {"reference": f"Encounter/{eid}"}}

            for key in e.dx:
                icd, snomed, disp = CONDITION_CODES[key]
                r = rng.random()
                codings = ([_coding(SYS_ICD10, icd, disp)] if r < 0.45 else
                           [_coding(SYS_SNOMED, snomed, disp)] if r < 0.75 else
                           [_coding(SYS_ICD10, icd, disp), _coding(SYS_SNOMED, snomed, disp)])
                cond_n += 1
                res["Condition"].append({"resourceType": "Condition", "id": f"rch-cond-{cond_n:08d}",
                                         "category": [{"coding": [{"code": "encounter-diagnosis"}]}],
                                         "code": {"coding": codings, "text": disp},
                                         "recordedDate": e.start.date().isoformat(),
                                         "onsetDateTime": e.start.isoformat(), **ref})

            for lab in e.labs:
                obs_n += 1
                loinc, disp = LAB_LOINC[lab.key]
                code = {"coding": [_coding(SYS_LOINC, loinc, disp)], "text": disp}
                value = _qty(lab.value, lab.unit)
                if lab.key == "A1C":
                    if rng.random() < 0.03:  # reported in IFCC units
                        ifcc = round((lab.value - 2.152) / 0.09148)
                        code = {"coding": [_coding(SYS_LOINC, "59261-8", "Hemoglobin A1c IFCC")], "text": "HbA1c"}
                        value = _qty(ifcc, "mmol/mol")
                    elif a1c_mislabels < 4 and rng.random() < 0.01:
                        value = _qty(round((lab.value - 2.152) / 0.09148), "%")  # mmol/mol number, "%" unit
                        a1c_mislabels += 1
                if lab.key == "EGFR" and e.start.year >= 2024 and rng.random() < 0.5:
                    code = {"coding": [_coding(SYS_LOCAL_LAB, "EGFR21", "eGFR CKD-EPI 2021 race-free")],
                            "text": "eGFR (CKD-EPI 2021, race-free)"}
                if lab.key == "GLU" and e.kind == "ER":
                    code = {"coding": [_coding(SYS_LOCAL_LAB, "GLU-POC", "Glucose, point of care")],
                            "text": "Glucose, point of care (fingerstick)"}
                obs = {"resourceType": "Observation", "id": f"rch-obs-{obs_n:08d}", "status": "final",
                       "category": [{"coding": [{"code": "laboratory"}]}], "code": code,
                       "effectiveDateTime": lab.time.isoformat(), "valueQuantity": value, **ref}
                res["Observation"].append(obs)
                if obs_n % 4000 == 0:
                    res["Observation"].append(json.loads(json.dumps(obs)))  # duplicate in export

            if e.vitals:
                obs_n += 1
                res["Observation"].append({
                    "resourceType": "Observation", "id": f"rch-obs-{obs_n:08d}", "status": "final",
                    "category": [{"coding": [{"code": "vital-signs"}]}],
                    "code": {"coding": [_coding(SYS_LOINC, "85354-9", "Blood pressure panel")]},
                    "effectiveDateTime": e.start.isoformat(), **ref,
                    "component": [
                        {"code": {"coding": [_coding(SYS_LOINC, "8480-6", "Systolic blood pressure")]},
                         "valueQuantity": _qty(e.vitals["SBP"], "mm[Hg]")},
                        {"code": {"coding": [_coding(SYS_LOINC, "8462-4", "Diastolic blood pressure")]},
                         "valueQuantity": _qty(e.vitals["DBP"], "mm[Hg]")}]})
                pounds = rng.random() < 0.3
                obs_n += 1
                res["Observation"].append({
                    "resourceType": "Observation", "id": f"rch-obs-{obs_n:08d}", "status": "final",
                    "category": [{"coding": [{"code": "vital-signs"}]}],
                    "code": {"coding": [_coding(SYS_LOINC, "29463-7", "Body weight")]},
                    "effectiveDateTime": e.start.isoformat(), **ref,
                    "valueQuantity": _qty(round(e.vitals["WT"] * 2.20462, 1), "[lb_av]") if pounds
                    else _qty(e.vitals["WT"], "kg")})
                obs_n += 1
                res["Observation"].append({
                    "resourceType": "Observation", "id": f"rch-obs-{obs_n:08d}", "status": "final",
                    "category": [{"coding": [{"code": "vital-signs"}]}],
                    "code": {"coding": [_coding(SYS_LOINC, "8302-2", "Body height")]},
                    "effectiveDateTime": e.start.isoformat(), **ref,
                    "valueQuantity": _qty(round(e.vitals["HT"] / 2.54, 1), "[in_i]") if pounds
                    else _qty(e.vitals["HT"], "cm")})

            for key in e.procs:
                if key in CPT:
                    proc_n += 1
                    code, disp = CPT[key]
                    res["Procedure"].append({"resourceType": "Procedure", "id": f"rch-proc-{proc_n:08d}",
                                             "status": "completed", "code": {"coding": [_coding(SYS_CPT, code, disp)]},
                                             "performedDateTime": e.start.isoformat(), **ref})

            for f in e.fills:
                med_n += 1
                rx, disp = RX[f.key]
                concept = {"coding": [_coding(SYS_RXNORM, rx, disp)], "text": disp}
                if f.key == "METFORMIN_500" and rng.random() < 0.1:
                    concept = {"text": "Metformin ER 500mg tab"}  # free text, no code
                res["MedicationRequest"].append({
                    "resourceType": "MedicationRequest", "id": f"rch-mr-{med_n:08d}", "status": "active",
                    "intent": "order", "medicationCodeableConcept": concept, "authoredOn": f.start.isoformat(),
                    "dispenseRequest": {"validityPeriod": {"start": f.start.isoformat()},
                                        "quantity": {"value": f.quantity},
                                        "expectedSupplyDuration": {"value": f.days_supply, "unit": "days"},
                                        "numberOfRepeatsAllowed": f.refills}, **ref})

    files = []
    for rtype, rows in res.items():
        path = out / f"{rtype}.ndjson"
        with open(path, "w") as fh:
            for r in rows:
                fh.write(json.dumps(r, separators=(",", ":")) + "\n")
        files.append({"file": path.name, "rows": len(rows), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    manifest = {"site": SITE, "format": "fhir-r4-bulk-ndjson",
                "transactionTime": "2026-01-01T00:00:00Z", "files": files}
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest


def generate(out: Path, n_patients: int = 5000, seed: int = 4242) -> dict:
    patients = simulate_site(SITE, n_patients, seed)
    write_truth(patients, SITE, out.parent / "_ground_truth")
    return write_fhir(patients, out)
