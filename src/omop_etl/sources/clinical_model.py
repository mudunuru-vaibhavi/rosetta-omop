"""A small clinical simulator shared by both fictional health systems.

It produces patient histories in a neutral, source-agnostic form (conditions, encounters, labs, vitals,
prescriptions). Each site's writer (clarity.py, fhir.py) then serialises the same kind of history in
its own native shape and coding, which is exactly the problem OMOP exists to solve.

The model is built so the research question in study/ has a known answer:

    Among adults newly diagnosed with type 2 diabetes, does starting a sulfonylurea (glipizide)
    instead of metformin raise the hazard of a hypoglycemia event?

* The true hazard ratio for glipizide vs metformin is planted as TRUE_HR (2.0).
* Older patients and patients with chronic kidney disease are more likely to get glipizide (metformin
  is avoided in poor kidney function) AND are at higher hypoglycemia risk on their own. That is
  confounding: a naive comparison overstates the drug effect, and adjustment should pull the
  estimate back toward 2.0. The study checks that it does.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

import numpy as np

TRUE_HR = 2.0  # planted hazard ratio, glipizide vs metformin, for hypoglycemia
BASE_HYPO_RATE = 0.07  # events per person-year on metformin at age 60 without CKD
WINDOW_START = date(2022, 1, 1)
WINDOW_END = date(2025, 12, 31)

CHRONIC = ["HTN", "HLD", "OBESITY", "CKD3", "ASTHMA", "MDD", "EXSMOKER", "BPH"]
CHRONIC_MEDS = {"HTN": "LISINOPRIL_10", "HLD": "ATORVASTATIN_20", "MDD": "SERTRALINE_50"}


@dataclass
class Lab:
    key: str  # A1C, GLU, CREAT, EGFR, LDL
    value: float
    unit: str
    time: datetime


@dataclass
class Fill:
    """One prescription fill: what OMOP calls a drug exposure."""

    key: str  # METFORMIN_500, GLIPIZIDE_5, ...
    start: date
    days_supply: int
    quantity: int
    refills: int


@dataclass
class Encounter:
    seq: int
    kind: str  # OP, TELE, ER, IP
    start: datetime
    end: datetime
    dept: str
    provider: int
    dx: list[str] = field(default_factory=list)  # condition keys; HYPO_DM = hypoglycemia in a diabetic
    procs: list[str] = field(default_factory=list)  # CPT keys
    labs: list[Lab] = field(default_factory=list)
    vitals: dict[str, float] = field(default_factory=dict)  # SBP, DBP, WT (kg), HT (cm)
    fills: list[Fill] = field(default_factory=list)


@dataclass
class Patient:
    idx: int
    sex: str
    birth: date
    race: str
    ethnicity: str
    zip: str
    enroll_start: date
    enroll_end: date
    death: date | None
    conditions: dict[str, date] = field(default_factory=dict)
    encounters: list[Encounter] = field(default_factory=list)
    truth: dict = field(default_factory=dict)  # ground truth for tests and the study validation


def _sigmoid(x: float) -> float:
    return 1 / (1 + math.exp(-x))


def _dt(d: date, rng: np.random.Generator, lo: int = 8, hi: int = 17) -> datetime:
    return datetime(d.year, d.month, d.day, int(rng.integers(lo, hi)), int(rng.integers(0, 4)) * 15)


class _Builder:
    def __init__(self, site: str, rng: np.random.Generator, n_providers: int):
        self.site = site
        self.rng = rng
        self.n_providers = n_providers
        self.enc_seq = 0

    def enc(self, kind: str, day: date, dept: str, los_days: int = 0) -> Encounter:
        self.enc_seq += 1
        start = _dt(day, self.rng, 0, 23) if kind in ("ER", "IP") else _dt(day, self.rng)
        if kind == "IP":
            end = start + timedelta(days=los_days, hours=int(self.rng.integers(2, 10)))
        elif kind == "ER":
            end = start + timedelta(hours=int(self.rng.integers(2, 8)))
        else:
            end = start + timedelta(minutes=30)
        return Encounter(self.enc_seq, kind, start, end, dept, int(self.rng.integers(0, self.n_providers)))


def _vitals(p: Patient, on: date, rng: np.random.Generator, base: dict) -> dict[str, float]:
    htn = "HTN" in p.conditions and p.conditions["HTN"] <= on
    sbp = rng.normal(142 if htn else 122, 12)
    dbp = rng.normal(88 if htn else 77, 8)
    wt = base["wt"] + rng.normal(0, 1.5)
    return {"SBP": round(sbp), "DBP": round(dbp), "WT": round(wt, 1), "HT": base["ht"]}


def simulate_site(site: str, n_patients: int, seed: int, n_providers: int = 24) -> list[Patient]:
    """Simulate `n_patients` adults for one site. Deterministic for a given seed."""
    rng = np.random.default_rng(seed)
    b = _Builder(site, rng, n_providers)
    rch = 1.0 if site == "RCH" else 0.0
    patients: list[Patient] = []

    for i in range(n_patients):
        age0 = float(np.clip(rng.triangular(30, 58, 88), 30, 88))
        sex = "F" if rng.random() < 0.52 else "M"
        birth = WINDOW_START - timedelta(days=int(age0 * 365.25))
        race = rng.choice(["5", "3", "2", "UNK"], p=[0.66, 0.2, 0.08, 0.06] if rch else [0.74, 0.15, 0.06, 0.05])
        eth = "Hispanic" if rng.random() < (0.12 if rch else 0.06) else "Not Hispanic"
        enroll_start = WINDOW_START + timedelta(days=int(rng.integers(0, 540)))
        enroll_end = WINDOW_END
        if rng.random() < 0.15:  # moved away / changed health system
            enroll_end = enroll_start + timedelta(days=int(rng.integers(400, (WINDOW_END - enroll_start).days)))

        p = Patient(i, sex, birth, str(race), eth, "", enroll_start, enroll_end, None)
        p.zip = str(rng.choice(["43215", "43017", "43081", "43123", "43026"] if not rch else
                               ["45202", "45231", "45040", "45069", "45011"]))

        # Death (age-dependent, rare)
        annual_death = 0.004 * math.exp(0.07 * (age0 - 60))
        t_death = min(rng.exponential(1 / annual_death) * 365.25, 36500)
        death_day = enroll_start + timedelta(days=int(t_death))
        if death_day < enroll_end:
            p.death = death_day
            p.enroll_end = death_day

        # Chronic conditions present before the window (onset before enrolment)
        probs = {
            "HTN": 0.18 + 0.007 * (age0 - 30), "HLD": 0.15 + 0.006 * (age0 - 30), "OBESITY": 0.30,
            "CKD3": 0.02 + 0.0045 * (age0 - 30), "ASTHMA": 0.08, "MDD": 0.10, "EXSMOKER": 0.20,
            "BPH": (0.30 if sex == "M" and age0 > 55 else 0.0),
        }
        for key, pr in probs.items():
            if rng.random() < pr:
                p.conditions[key] = enroll_start - timedelta(days=int(rng.integers(200, 4000)))
        ckd = 1.0 if "CKD3" in p.conditions else 0.0

        # Type 2 diabetes: prevalent (diagnosed before the window) or incident (new, inside it)
        prevalent_dm = rng.random() < 0.10 + 0.004 * (age0 - 30)
        dm_drug_prevalent = None
        if prevalent_dm:
            p.conditions["T2DM"] = enroll_start - timedelta(days=int(rng.integers(400, 3000)))
            dm_drug_prevalent = "METFORMIN_500" if rng.random() < 0.7 else "GLIPIZIDE_5"
        incident_day = None
        latest = min(p.enroll_end - timedelta(days=120), date(2025, 6, 30))
        earliest = enroll_start + timedelta(days=380)
        if not prevalent_dm and earliest < latest and rng.random() < 0.38:
            incident_day = earliest + timedelta(days=int(rng.integers(0, (latest - earliest).days)))
            p.conditions["T2DM"] = incident_day

        base = {"ht": round(rng.normal(163 if sex == "F" else 176, 7), 1)}
        base["wt"] = rng.normal(78 if sex == "F" else 90, 14) + (18 if "OBESITY" in p.conditions else 0)

        # Visit schedule
        n_chronic = sum(k in p.conditions for k in CHRONIC) + (1 if "T2DM" in p.conditions else 0)
        gap = 110 if n_chronic >= 2 or "T2DM" in p.conditions else 260
        day = enroll_start + timedelta(days=int(rng.integers(0, 30)))
        visit_days = []
        while day <= p.enroll_end:
            visit_days.append(day)
            day += timedelta(days=max(20, int(rng.normal(gap, gap * 0.25))))
        if incident_day:
            visit_days.append(incident_day)
            visit_days.append(incident_day + timedelta(days=int(rng.integers(80, 100))))
            visit_days = sorted(d for d in set(visit_days) if d <= p.enroll_end)

        for vd in visit_days:
            tele = rng.random() < 0.12 and vd != incident_day
            e = b.enc("TELE" if tele else "OP", vd, "PRIMARY CARE")
            active = [k for k, on in p.conditions.items() if on <= vd]
            e.dx = [k for k in active if rng.random() < 0.7] or (["T2DM"] if vd == incident_day else [])
            if vd == incident_day and "T2DM" not in e.dx:
                e.dx.insert(0, "T2DM")
            if not tele:
                e.vitals = _vitals(p, vd, rng, base)
            dm_active = "T2DM" in active
            on_tx = dm_active and (incident_day is None or vd > incident_day)
            if dm_active and (rng.random() < 0.6 or vd == incident_day):
                a1c = rng.normal(8.4, 1.0) if not on_tx else rng.normal(7.2, 0.8)
                e.labs.append(Lab("A1C", round(float(np.clip(a1c, 4.8, 13.9)), 1), "%", e.start))
                e.procs.append("CPT_A1C")
            if dm_active and rng.random() < 0.4:
                e.labs.append(Lab("GLU", round(rng.normal(150 if not on_tx else 128, 25)), "mg/dL", e.start))
            if (ckd or dm_active or "HTN" in active) and rng.random() < 0.4:
                egfr = rng.uniform(32, 58) if ckd else rng.uniform(62, 110)
                e.labs.append(Lab("EGFR", round(egfr), "mL/min/{1.73_m2}", e.start))
                e.labs.append(Lab("CREAT", round(float(np.clip(95 / egfr, 0.5, 3.5)), 2), "mg/dL", e.start))
            if "HLD" in active and rng.random() < 0.3:
                e.labs.append(Lab("LDL", round(rng.normal(118, 28)), "mg/dL", e.start))
                e.procs.append("CPT_LIPID")
            if e.labs and not tele:
                e.procs.append("CPT_VENI")
            if rng.random() < 0.04:
                e.procs.append("CPT_ECG")
            if sex == "F" and age0 < 42 and rng.random() < 0.02:
                e.dx.append("PREG")
            p.encounters.append(e)

        # Prescriptions: chronic meds and prevalent diabetes drug, filled about every 90 days
        def fills(key: str, start: date, stop: date, enc_pool: list[Encounter]) -> None:
            d = start
            while d < stop:
                if rng.random() > 0.08:  # occasional missed fill
                    host = min(enc_pool, key=lambda x: abs((x.start.date() - d).days)) if enc_pool else None
                    f = Fill(key, d, 90, 90 if key != "GLARGINE_100" else 3, int(rng.integers(0, 3)))
                    (host.fills if host else p.encounters[0].fills).append(f)  # noqa: B023 (called within this iteration)
                d += timedelta(days=int(rng.normal(92, 6)))

        outpatient = [e for e in p.encounters if e.kind in ("OP", "TELE")]
        if outpatient:
            for cond, med in CHRONIC_MEDS.items():
                if cond in p.conditions and rng.random() < 0.75:
                    fills(med, outpatient[0].start.date(), p.enroll_end, outpatient)
            if dm_active_prev := dm_drug_prevalent:
                fills(dm_active_prev, outpatient[0].start.date(), p.enroll_end, outpatient)

        # Incident diabetes: choose first-line drug (confounded by age and kidney function)
        if incident_day:
            age_dx = (incident_day - birth).days / 365.25
            p_su = _sigmoid(-1.1 + 0.045 * (age_dx - 60) + 1.3 * ckd + 0.4 * rch)
            arm = "GLIPIZIDE_5" if rng.random() < p_su else "METFORMIN_500"
            dx_enc = next(e for e in p.encounters if e.start.date() == incident_day)
            fills(arm, incident_day, p.enroll_end, [dx_enc] + [e for e in outpatient if e.start.date() > incident_day])
            if rng.random() < 0.3:
                dx_enc.procs.append("CPT_DSME")
            if rng.random() < 0.08:  # some later add basal insulin
                fills("GLARGINE_100", incident_day + timedelta(days=int(rng.integers(250, 500))), p.enroll_end,
                      outpatient)

            # Hypoglycemia: exponential time-to-event with the planted hazard ratio
            su = 1.0 if arm == "GLIPIZIDE_5" else 0.0
            lam = BASE_HYPO_RATE * math.exp(math.log(TRUE_HR) * su + 0.025 * (age_dx - 60) + 0.55 * ckd)
            t_event = rng.exponential(1 / lam) * 365.25
            hypo_day = incident_day + timedelta(days=max(3, int(t_event)))
            p.truth = {"incident_t2dm": incident_day, "arm": arm, "ckd": bool(ckd), "age_dx": age_dx}
            if hypo_day < p.enroll_end:
                p.truth["hypo_day"] = hypo_day
                admit = rng.random() < 0.2
                e = b.enc("IP" if admit else "ER", hypo_day, "INPATIENT MEDICINE" if admit else "EMERGENCY",
                          los_days=int(rng.integers(1, 4)))
                e.dx = ["HYPO_DM"]
                e.labs.append(Lab("GLU", round(rng.uniform(32, 62)), "mg/dL", e.start))
                p.encounters.append(e)

        # Rare acute events
        if rng.random() < 0.012 * (1 + ckd) * (1 + (age0 > 65)):
            d = enroll_start + timedelta(days=int(rng.integers(30, max(31, (p.enroll_end - enroll_start).days))))
            if d < p.enroll_end:
                e = b.enc("IP", d, "INPATIENT MEDICINE", los_days=int(rng.integers(3, 7)))
                e.dx = ["AMI"] + (["HTN"] if "HTN" in p.conditions else [])
                e.procs = ["CPT_ECG"]
                p.encounters.append(e)
                p.conditions.setdefault("AMI", d)

        p.encounters.sort(key=lambda e: e.start)
        patients.append(p)
    return patients


def write_truth(patients: list[Patient], site: str, out_dir) -> None:
    """Save the simulator's ground truth for the study population (never read by the ETL).

    The study compares its OMOP-based estimate against an "oracle" analysis of these true values,
    which isolates what the ETL and cohort logic contribute from ordinary sampling noise.
    """
    import csv
    from pathlib import Path

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    with open(out_dir / f"{site}_study_truth.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["site", "patient_idx", "incident_t2dm", "arm", "ckd", "age_dx", "hypo_day", "enroll_end"])
        for p in patients:
            t = p.truth
            if "incident_t2dm" in t:
                w.writerow([site, p.idx, t["incident_t2dm"], t["arm"], int(t["ckd"]), round(t["age_dx"], 2),
                            t.get("hypo_day", ""), p.enroll_end])
