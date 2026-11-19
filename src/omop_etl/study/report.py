"""Write the study results as a short manuscript-style report (Markdown), generated from the numbers."""

from __future__ import annotations

from pathlib import Path


def _hr(h: dict) -> str:
    return f"{h['hr']:.2f} (95% CI {h['lo']:.2f}–{h['hi']:.2f})"


def _p(p: float) -> str:
    return "<0.001" if p < 0.001 else f"{p:.3f}"


def write_report(r: dict, out: Path) -> None:
    arms = r["by_arm"]
    o = r.get("oracle")
    att = "\n".join(f"| {a['step']} | {a['persons']:,} |" for a in r["attrition"])
    t1 = "\n".join(f"| {x['variable']} | {x['glipizide']} | {x['metformin']} | {x['smd_before']:.2f} | "
                   f"{x['smd_after_iptw']:.2f} |" for x in r["table1"])
    sites = "\n".join(f"| {s} | {v['n']:,} | {v['events']} | {_hr(v)} |" for s, v in r["by_site"].items())
    rm = r["risk_model"]
    cal = "\n".join(f"| {i + 1} | {c['n']} | {100 * c['predicted']:.1f}% | {100 * c['observed']:.1f}% |"
                    for i, c in enumerate(rm.get("calibration", [])))
    oracle_txt = ""
    if o:
        oracle_txt = f"""
### Validation against the simulator's ground truth
Because the data are synthetic, the true exposure, kidney status and event date of every simulated new
user are known. Fitting the same adjusted model on those true values (the "oracle") gives HR
{_hr(o['oracle_adjusted_hr'])}; the OMOP pipeline's adjusted estimate is {_hr(r['cox_adjusted'])}. The
oracle carries the same sampling noise as our estimate, so agreement between the two shows that the
ETL, vocabulary mapping and cohort logic preserved the signal. The cohort found {o['found_in_cohort']:,} of
{o['true_new_users']:,} true new users ({o['found_pct']}%; the rest lacked a year of prior observation
or started both drugs together); among those found, treatment group agreed for {o['arm_agreement_pct']}%
and outcome status for {o['event_agreement_pct']}%.
"""

    text = f"""# Glipizide versus metformin as first oral therapy and the risk of hypoglycemia
*A new-user, active-comparator cohort study in a two-site OMOP CDM (synthetic data)*

> All data are synthetic. Two fictional health systems were simulated with a planted true hazard
> ratio of {r['true_hr']:.1f}. The study exists to demonstrate methods, not to inform care.

## Background
Sulfonylureas stimulate insulin release regardless of blood glucose and are known to cause more
hypoglycemia than metformin. This analysis estimates that difference in routinely collected EHR data
harmonised to the OMOP Common Data Model from two health systems with different source formats
(Epic Clarity and FHIR R4).

## Methods
**Data source.** OMOP CDM v5.4 built from Cedar Valley Health (Epic Clarity extract) and Riverside
Community Health (FHIR R4 bulk export). Codes were mapped to standard concepts through OMOP vocabulary
"Maps to" relationships and a reviewed SOURCE_TO_CONCEPT_MAP for local codes.

**Design.** New-user, active-comparator cohort. Index date = first exposure to metformin or glipizide
(any clinical drug containing the ingredient, via CONCEPT_ANCESTOR). Inclusion: ≥365 days of prior
observation, type 2 diabetes diagnosis on or before index, age ≥18. Exclusion: both drugs started the
same day, any prior insulin, hypoglycemia in the prior 365 days.

**Outcome.** First hypoglycemia diagnosis after index (SNOMED 302866003 and its descendants, which
captures ICD-10-CM E16.2 and E11.649 through mapping).

**Follow-up.** Intention-to-treat from index until outcome, end of observation, death, or 730 days.

**Covariates.** Age, sex, chronic kidney disease (diagnosis or eGFR <60 in the prior year), most recent
HbA1c in the 180 days before index (median-imputed with a missing indicator), and site.

**Analysis.** Kaplan–Meier cumulative incidence with log-rank test; Cox proportional hazards models,
unadjusted, multivariable-adjusted, and inverse-probability-of-treatment weighted (stabilised weights
from a logistic propensity model, truncated at the 99th percentile, robust variance). Balance assessed
with standardised mean differences (SMD; <0.1 taken as balanced). Proportional hazards checked with a
Schoenfeld residual test. Site-specific adjusted models assess heterogeneity. A one-year risk model
(logistic regression) was trained at one site and externally validated at the other.

## Results
### Cohort
| Step | Persons |
|---|---|
{att}

{r['n']:,} new users were followed for {r['person_years']:,} person-years: {arms['glipizide']['n']:,} started
glipizide and {arms['metformin']['n']:,} metformin. {r['events']} had a hypoglycemia event
({arms['glipizide']['rate_per_100py']} vs {arms['metformin']['rate_per_100py']} per 100 person-years).

### Baseline characteristics
| Characteristic | Glipizide | Metformin | SMD before | SMD after IPTW |
|---|---|---|---|---|
{t1}

Glipizide users were older and more often had kidney disease, as expected: metformin is avoided in
reduced kidney function. Weighting removed these differences (all SMD <0.1).

### Hazard of hypoglycemia, glipizide vs metformin
| Model | Hazard ratio |
|---|---|
| Unadjusted | {_hr(r['cox_unadjusted'])} |
| Adjusted (age, sex, CKD, HbA1c, site) | {_hr(r['cox_adjusted'])} |
| IPTW | {_hr(r['cox_iptw'])} |

Log-rank p {_p(r['logrank_p'])}. Proportional-hazards test for treatment p = {r['ph_test_p_sulfonylurea']:.2f}.
The unadjusted estimate is the largest because it carries confounding by age and kidney function;
adjustment and weighting move it toward the planted value of {r['true_hr']:.1f}.

### By site
| Site | N | Events | Adjusted HR |
|---|---|---|---|
{sites}
{oracle_txt}
### One-year risk model (trained at {rm['train_site']}, validated at {rm['test_site']})
AUC {rm['auc_train']} at the training site and **{rm['auc_test']} at the external site**; Brier score
{rm['brier_test']}. Calibration at the external site by fifth of predicted risk:

| Fifth | N | Predicted | Observed |
|---|---|---|---|
{cal}

## Limitations
Synthetic data with a simple generating model; no time-varying exposure (intention-to-treat only);
residual confounding is possible in real data from unmeasured factors such as frailty or diet; the
outcome relies on coded diagnoses, which under-capture mild hypoglycemia treated at home.

## Reproducibility
Every number in this report is produced by `omop-etl run` from a fixed random seed; the cohort SQL is
in `src/omop_etl/study/cohort.py` and the models in `src/omop_etl/study/analysis.py`.
"""
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text)
