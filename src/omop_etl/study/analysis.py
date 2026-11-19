"""Step 5b: time-to-event analysis and a validated risk model (lifelines, scikit-learn).

Analyses
    1. Table 1: baseline characteristics by treatment group, with standardized mean differences (SMD).
       An SMD above 0.1 means the groups are meaningfully different on that characteristic.
    2. Kaplan-Meier: share of each group still free of hypoglycemia over two years; log-rank test.
    3. Cox proportional hazards models for the hazard ratio (HR) of glipizide vs metformin:
         a. unadjusted
         b. adjusted for age, sex, CKD, baseline HbA1c and site
         c. inverse probability of treatment weighting (IPTW): a propensity model predicts each person's
            chance of getting glipizide; weighting by its inverse builds a pseudo-population in which the
            groups are balanced, then a weighted Cox model compares them.
       The synthetic data was generated with a TRUE hazard ratio of 2.0, so we can see which analysis
       recovers it. The unadjusted estimate is biased upward by confounding (older and CKD patients get
       glipizide more often and have more hypoglycemia anyway).
    4. Site-specific adjusted HRs (is the effect consistent across health systems?).
    5. One-year hypoglycemia risk model: logistic regression trained on Cedar Valley Health and validated
       on Riverside Community Health (external, cross-site validation): AUC, Brier score, calibration.
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
from lifelines import CoxPHFitter, KaplanMeierFitter
from lifelines.statistics import logrank_test
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, roc_auc_score

from ..sources.clinical_model import TRUE_HR

COVARIATES = ["age", "female", "ckd", "baseline_a1c"]


def _prepare(cohort: pd.DataFrame) -> pd.DataFrame:
    df = cohort.copy()
    df["a1c_missing"] = df["baseline_a1c"].isna().astype(int)
    df["baseline_a1c"] = df["baseline_a1c"].fillna(df["baseline_a1c"].median())
    df["site_rch"] = (df["site"] == "RCH").astype(int)
    df["time_years"] = df["time_days"] / 365.25
    return df


def _varying(df: pd.DataFrame, cols: list[str]) -> list[str]:
    """Drop covariates that are constant in this data (e.g. no missing HbA1c), which would make models singular."""
    return [c for c in cols if df[c].nunique() > 1]


def smd(x1: pd.Series, x0: pd.Series, w1=None, w0=None) -> float:
    def wstats(x, w):
        w = np.ones(len(x)) if w is None else np.asarray(w)
        m = np.average(x, weights=w)
        return m, np.average((x - m) ** 2, weights=w)
    m1, v1 = wstats(x1.to_numpy(float), w1)
    m0, v0 = wstats(x0.to_numpy(float), w0)
    return float((m1 - m0) / math.sqrt((v1 + v0) / 2)) if v1 + v0 > 0 else 0.0


def table_one(df: pd.DataFrame, weights: np.ndarray) -> list[dict]:
    rows = []
    t, c = df["sulfonylurea"] == 1, df["sulfonylurea"] == 0
    for var, label, kind in [("age", "Age, years (mean)", "mean"), ("female", "Female, %", "pct"),
                             ("ckd", "Chronic kidney disease, %", "pct"),
                             ("baseline_a1c", "Baseline HbA1c, % (mean)", "mean"),
                             ("site_rch", "Riverside Community Health, %", "pct")]:
        f = 100 if kind == "pct" else 1
        rows.append({"variable": label,
                     "glipizide": round(df.loc[t, var].mean() * f, 1),
                     "metformin": round(df.loc[c, var].mean() * f, 1),
                     "smd_before": round(smd(df.loc[t, var], df.loc[c, var]), 3),
                     "smd_after_iptw": round(smd(df.loc[t, var], df.loc[c, var], weights[t], weights[c]), 3)})
    return rows


def _hr(cph: CoxPHFitter, var: str = "sulfonylurea") -> dict:
    s = cph.summary.loc[var]
    return {"hr": round(float(s["exp(coef)"]), 3), "lo": round(float(s["exp(coef) lower 95%"]), 3),
            "hi": round(float(s["exp(coef) upper 95%"]), 3), "p": float(s["p"])}


def run_analysis(cohort: pd.DataFrame) -> dict:
    df = _prepare(cohort)
    out: dict = {"n": int(len(df)), "events": int(df["event"].sum()), "true_hr": TRUE_HR,
                 "person_years": round(float(df["time_years"].sum()), 1)}
    out["by_arm"] = {
        arm: {"n": int((g := df[df["sulfonylurea"] == v]).shape[0]), "events": int(g["event"].sum()),
              "person_years": round(float(g["time_years"].sum()), 1),
              "rate_per_100py": round(100 * g["event"].sum() / g["time_years"].sum(), 2)}
        for arm, v in (("glipizide", 1), ("metformin", 0))}

    # Kaplan-Meier, sampled every 15 days for plotting
    grid = np.arange(0, 731, 15) / 365.25
    km = {}
    for arm, v in (("glipizide", 1), ("metformin", 0)):
        g = df[df["sulfonylurea"] == v]
        k = KaplanMeierFitter().fit(g["time_years"], g["event"])
        ci = k.confidence_interval_survival_function_
        km[arm] = {"t_days": (grid * 365.25).round().astype(int).tolist(),
                   "cum_incidence": [round(1 - float(k.survival_function_at_times(x).iloc[0]), 4) for x in grid],
                   "lo": [round(1 - float(np.interp(x, ci.index, ci.iloc[:, 1])), 4) for x in grid],
                   "hi": [round(1 - float(np.interp(x, ci.index, ci.iloc[:, 0])), 4) for x in grid]}
    out["km"] = km
    lr = logrank_test(df.loc[df.sulfonylurea == 1, "time_years"], df.loc[df.sulfonylurea == 0, "time_years"],
                      df.loc[df.sulfonylurea == 1, "event"], df.loc[df.sulfonylurea == 0, "event"])
    out["logrank_p"] = float(lr.p_value)

    cols = ["time_years", "event", "sulfonylurea"]
    adj = cols + _varying(df, COVARIATES + ["a1c_missing", "site_rch"])
    out["cox_unadjusted"] = _hr(CoxPHFitter().fit(df[cols], "time_years", "event"))
    cph_adj = CoxPHFitter().fit(df[adj], "time_years", "event")
    out["cox_adjusted"] = _hr(cph_adj)
    out["cox_adjusted_all"] = {k: _hr(cph_adj, k) for k in adj[2:]}

    # Propensity score and stabilized IPTW
    X = df[adj[3:]].to_numpy(float)
    ps = LogisticRegression(max_iter=1000).fit(X, df["sulfonylurea"]).predict_proba(X)[:, 1]
    p_treat = df["sulfonylurea"].mean()
    w = np.where(df["sulfonylurea"] == 1, p_treat / ps, (1 - p_treat) / (1 - ps))
    w = np.clip(w, None, np.percentile(w, 99))
    dfw = df[cols].assign(w=w)
    out["cox_iptw"] = _hr(CoxPHFitter().fit(dfw, "time_years", "event", weights_col="w", robust=True))
    out["propensity"] = {"glipizide": np.histogram(ps[df.sulfonylurea == 1], bins=20, range=(0, 1))[0].tolist(),
                         "metformin": np.histogram(ps[df.sulfonylurea == 0], bins=20, range=(0, 1))[0].tolist()}
    out["table1"] = table_one(df, w)

    # Proportional-hazards assumption (Schoenfeld residual test) for the adjusted model
    from lifelines.statistics import proportional_hazard_test
    ph = proportional_hazard_test(cph_adj, df[adj], time_transform="rank")
    out["ph_test_p_sulfonylurea"] = float(ph.summary.loc["sulfonylurea", "p"])

    out["by_site"] = {}
    for site in sorted(df["site"].unique()):
        g = df[df["site"] == site]
        out["by_site"][site] = {"n": int(len(g)), "events": int(g["event"].sum()),
                                **_hr(CoxPHFitter().fit(g[[c for c in adj if c != "site_rch"]], "time_years",
                                                         "event"))}

    out["risk_model"] = risk_model(df)
    return out


def risk_model(df: pd.DataFrame) -> dict:
    """1-year hypoglycemia risk: train on CVH, validate on RCH (external validation across sites)."""
    feats = _varying(df, ["sulfonylurea", *COVARIATES, "a1c_missing"])
    d = df[(df["event"] == 1) & (df["time_days"] <= 365) | (df["time_days"] >= 365)].copy()
    d["y"] = ((d["event"] == 1) & (d["time_days"] <= 365)).astype(int)
    train, test = d[d.site == "CVH"], d[d.site == "RCH"]
    model = LogisticRegression(max_iter=1000).fit(train[feats], train["y"])
    res = {"features": feats, "train_site": "CVH", "test_site": "RCH",
           "n_train": int(len(train)), "n_test": int(len(test)),
           "events_train": int(train.y.sum()), "events_test": int(test.y.sum()),
           "coefficients": {f: round(float(c), 3) for f, c in zip(feats, model.coef_[0], strict=True)}}
    for name, part in (("train", train), ("test", test)):
        p = model.predict_proba(part[feats])[:, 1]
        res[f"auc_{name}"] = round(float(roc_auc_score(part.y, p)), 3)
        res[f"brier_{name}"] = round(float(brier_score_loss(part.y, p)), 4)
        if name == "test":
            q = pd.qcut(p, 5, labels=False, duplicates="drop")
            cal = pd.DataFrame({"q": q, "p": p, "y": part.y.to_numpy()}).groupby("q").agg(
                predicted=("p", "mean"), observed=("y", "mean"), n=("y", "size"))
            res["calibration"] = [{k: (round(float(v), 4) if k != "n" else int(v)) for k, v in r.items()}
                                  for r in cal.reset_index(drop=True).to_dict("records")]
    return res


def oracle_check(cohort: pd.DataFrame, truth_dir) -> dict | None:
    """Compare the OMOP-derived cohort with the simulator's ground truth for the same people.

    * Oracle HR: the same adjusted Cox model fitted on the TRUE exposure, CKD, age and event times. It
      carries the same sampling noise as our estimate, so it is the right yardstick for the pipeline.
    * Fidelity: how many true new users the cohort found, and how many true events it captured.
    """
    from datetime import timedelta
    from pathlib import Path

    files = sorted(Path(truth_dir).glob("*_study_truth.csv")) if truth_dir else []
    if not files:
        return None
    t = pd.concat([pd.read_csv(f, parse_dates=["incident_t2dm", "hypo_day", "enroll_end"]) for f in files])
    t["person_source_value"] = np.where(
        t["site"] == "CVH", "CVH|Z" + (100000 + t["patient_idx"]).astype(str),
        "RCH|rch-pat-" + t["patient_idx"].astype(str).str.zfill(6))
    end = np.minimum(t["enroll_end"], t["incident_t2dm"] + timedelta(days=730))
    t["event"] = (t["hypo_day"].notna() & (t["hypo_day"] <= end)).astype(int)
    t["time_years"] = ((t["hypo_day"].where(t["event"] == 1, end) - t["incident_t2dm"]).dt.days.clip(lower=1)
                       / 365.25)
    t["sulfonylurea"] = (t["arm"] == "GLIPIZIDE_5").astype(int)
    t["age"] = t["age_dx"]
    oracle = _hr(CoxPHFitter().fit(t[["time_years", "event", "sulfonylurea", "age", "ckd"]], "time_years", "event"))

    psv = set(cohort["person_source_value"]) if "person_source_value" in cohort else set()
    found = t["person_source_value"].isin(psv)
    c = cohort.set_index("person_source_value") if psv else None
    agree_arm = agree_event = 0
    if c is not None:
        m = t[found].set_index("person_source_value").join(c[["sulfonylurea", "event"]], rsuffix="_omop")
        agree_arm = float((m["sulfonylurea"] == m["sulfonylurea_omop"]).mean())
        agree_event = float((m["event"] == m["event_omop"]).mean())
    return {"oracle_adjusted_hr": oracle, "true_new_users": int(len(t)), "found_in_cohort": int(found.sum()),
            "found_pct": round(100 * found.mean(), 1), "arm_agreement_pct": round(100 * agree_arm, 1),
            "event_agreement_pct": round(100 * agree_event, 1)}
