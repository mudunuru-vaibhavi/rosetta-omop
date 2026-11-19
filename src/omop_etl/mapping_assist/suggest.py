"""Assisted concept mapping for local codes, with human review.

The problem: some source codes exist in no public vocabulary (a lab's in-house test code, a formulary
item with no RxNorm code, free-text medication names). In OMOP these are handled with
SOURCE_TO_CONCEPT_MAP: a reviewed table mapping each local code to a standard concept. Building it by
hand is slow; accepting automatic matches unchecked is unsafe. This module does three things:

1. RETRIEVE  For each unmapped code, rank candidate standard concepts in the expected domain from the
             vocabulary itself (character n-gram TF-IDF on concept names, after expanding common clinical
             abbreviations). Only REAL concepts can be suggested.
2. VALIDATE  Deterministic checks on the top candidate: the concept exists, is standard and valid, is in
             the expected domain, the source unit agrees with what is already stored for that concept, and
             drug strength and release form agree (e.g. "ER" must not map to an immediate-release drug).
3. REVIEW    Suggestions are written to mappings/source_to_concept_map_review.csv with status "pending".
             Only a person changes a row to "approved" (optionally correcting the target). The ETL loads
             approved rows only, and this tool never overwrites a reviewed row.
"""

from __future__ import annotations

import csv
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path

import duckdb
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

EXPECTED_DOMAIN = {"lab": "Measurement", "vital": "Measurement", "drug": "Drug", "diagnosis": "Condition",
                   "procedure": "Procedure"}
REVIEW_COLUMNS = ["source_vocabulary_id", "source_code", "source_code_description", "site", "records",
                  "example_unit", "target_vocabulary_id", "target_concept_code", "target_concept_name",
                  "suggested_by", "confidence", "rationale", "validation", "status", "reviewer", "reviewed_on",
                  "review_note"]
# Abbreviations common in lab and pharmacy item names, expanded before matching
SYNONYMS = {
    r"\begfr\b": "glomerular filtration rate", r"\ba1c\b": "hemoglobin a1c", r"\bhgba1c\b": "hemoglobin a1c",
    r"\bglu\b": "glucose", r"\bpoc\b": "point of care blood", r"\bfingerstick\b": "blood",
    r"\btab\b": "oral tablet", r"\bldl\b": "cholesterol in ldl", r"\bckd\b": "kidney",
}


@dataclass
class Candidate:
    concept_id: int
    vocabulary_id: str
    concept_code: str
    concept_name: str
    concept_class_id: str
    score: float


@dataclass
class Suggestion:
    site: str
    source_vocabulary_id: str
    source_code: str
    description: str
    kind: str
    records: int
    unit: str | None
    candidates: list[Candidate] = field(default_factory=list)
    chosen: Candidate | None = None
    suggested_by: str = ""
    confidence: str = ""
    rationale: str = ""
    validation: list[str] = field(default_factory=list)


def _normalise(text: str) -> str:
    t = (text or "").lower()
    for pat, rep in SYNONYMS.items():
        t = re.sub(pat, rep, t)
    return re.sub(r"[^a-z0-9.% ]+", " ", t)


def unmapped_codes(con: duckdb.DuckDBPyConnection) -> list[dict]:
    rows = con.execute("""
        SELECT site, event_kind, vocabulary_id, code, source_description, records, example_unit
        FROM etl.source_coverage WHERE NOT mapped ORDER BY records DESC""").fetchall()
    return [dict(zip(["site", "kind", "vocabulary_id", "code", "description", "records", "unit"], r, strict=True))
            for r in rows]


def retrieve(con: duckdb.DuckDBPyConnection, description: str, domain: str, k: int = 5) -> list[Candidate]:
    rows = con.execute("""
        SELECT concept_id, vocabulary_id, concept_code, concept_name, concept_class_id FROM cdm.concept
        WHERE domain_id = ? AND standard_concept = 'S' AND COALESCE(invalid_reason, '') = ''""", [domain]).fetchall()
    if not rows:
        return []
    names = [_normalise(r[3]) for r in rows]
    vec = TfidfVectorizer(analyzer="char_wb", ngram_range=(3, 5)).fit(names + [_normalise(description)])
    sims = cosine_similarity(vec.transform([_normalise(description)]), vec.transform(names))[0]
    ranked = sorted(zip(rows, sims, strict=True), key=lambda x: -x[1])[:k]
    return [Candidate(int(r[0]), r[1], r[2], r[3], r[4], round(float(s), 3)) for r, s in ranked]


def validate(con: duckdb.DuckDBPyConnection, s: Suggestion) -> list[str]:
    """Deterministic checks; each line starts with 'ok:', 'WARN:' or 'FAIL:'."""
    if s.chosen is None:
        return ["FAIL: no concept chosen"]
    c = s.chosen
    out = []
    row = con.execute("SELECT domain_id, standard_concept, COALESCE(invalid_reason, '') FROM cdm.concept "
                      "WHERE concept_id = ?", [c.concept_id]).fetchone()
    if row is None:
        return ["FAIL: concept does not exist in the vocabulary"]
    domain, std, invalid = row
    out.append("ok: concept exists")
    out.append("ok: standard and valid" if std == "S" and not invalid else "FAIL: not a valid standard concept")
    exp = EXPECTED_DOMAIN.get(s.kind)
    out.append(f"ok: domain {domain}" if domain == exp else f"FAIL: domain {domain}, expected {exp}")
    if exp == "Measurement" and s.unit:
        units = [r[0] for r in con.execute(
            "SELECT DISTINCT u.concept_code FROM cdm.measurement m JOIN cdm.concept u ON u.concept_id = m.unit_concept_id "
            "WHERE m.measurement_concept_id = ? AND m.unit_concept_id <> 0", [c.concept_id]).fetchall()]
        if not units:
            out.append(f"WARN: no stored results for this concept to compare unit '{s.unit}'")
        elif s.unit in units:
            out.append(f"ok: unit {s.unit} matches stored results")
        else:
            out.append(f"FAIL: unit {s.unit} differs from stored units {units}")
    if exp == "Drug":
        src_er = bool(re.search(r"\b(er|xr|xl|sr|extended|24 hr)\b", s.description.lower()))
        tgt_er = bool(re.search(r"extended|24 hr|\ber\b", c.concept_name.lower()))
        if src_er != tgt_er:
            out.append("FAIL: release form differs (extended vs immediate release)")
        strengths = set(re.findall(r"(\d+(?:\.\d+)?)\s*(?:mg|unit)", s.description.lower()))
        tgt_strengths = set(re.findall(r"(\d+(?:\.\d+)?)\s*(?:mg|unt)", c.concept_name.lower()))
        if strengths and tgt_strengths and not strengths & tgt_strengths:
            out.append(f"FAIL: strength differs ({strengths} vs {tgt_strengths})")
        elif strengths:
            out.append("ok: strength matches")
    return out


def suggest(db_path: Path, review_csv: Path) -> list[Suggestion]:
    con = duckdb.connect(str(db_path), read_only=True)
    existing = {(r["source_vocabulary_id"], r["source_code"]) for r in read_review(review_csv)}
    out = []
    for u in unmapped_codes(con):
        if (u["vocabulary_id"], u["code"]) in existing:
            continue  # already suggested or reviewed: never overwrite a person's decision
        s = Suggestion(u["site"], u["vocabulary_id"], u["code"], u["description"] or u["code"], u["kind"],
                       int(u["records"]), u["unit"])
        s.candidates = retrieve(con, s.description, EXPECTED_DOMAIN.get(s.kind, "Measurement"))
        if s.candidates:
            s.chosen = s.candidates[0]
            s.confidence = "high" if s.chosen.score > 0.6 else "medium" if s.chosen.score > 0.35 else "low"
            s.rationale = f"closest concept name by character n-gram similarity ({s.chosen.score})"
            s.suggested_by = "tfidf-retrieval"
        s.validation = validate(con, s)
        out.append(s)
    con.close()
    append_review(review_csv, out)
    return out


def read_review(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def append_review(path: Path, suggestions: list[Suggestion]) -> None:
    if not suggestions:
        return  # nothing new: leave the reviewed file untouched
    rows = read_review(path)
    for s in suggestions:
        c = s.chosen
        rows.append({
            "source_vocabulary_id": s.source_vocabulary_id, "source_code": s.source_code,
            "source_code_description": s.description, "site": s.site, "records": s.records,
            "example_unit": s.unit or "", "target_vocabulary_id": c.vocabulary_id if c else "",
            "target_concept_code": c.concept_code if c else "", "target_concept_name": c.concept_name if c else "",
            "suggested_by": s.suggested_by, "confidence": s.confidence, "rationale": s.rationale,
            "validation": " | ".join(s.validation), "status": "pending", "reviewer": "", "reviewed_on": "",
            "review_note": "",
        })
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=REVIEW_COLUMNS)
        w.writeheader()
        w.writerows(rows)


def to_json(suggestions: list[Suggestion]) -> list[dict]:
    return [asdict(s) for s in suggestions]
