import hashlib
import json
import re

import pytest

from omop_etl.sources import clarity, fhir
from omop_etl.storage import ManifestError, land, verify_manifest
from omop_etl.vocab.demo import LOCAL_ID_START, build_rows


def test_demo_vocabulary_is_consistent():
    v = build_rows()
    ids = [c["concept_id"] for c in v["CONCEPT"]]
    assert len(ids) == len(set(ids)), "concept ids must be unique"
    assert all(i == 0 or i >= LOCAL_ID_START for i in ids), "demo ids stay in the local (2-billion) range"
    by_id = {c["concept_id"]: c for c in v["CONCEPT"]}
    for r in v["CONCEPT_RELATIONSHIP"]:
        if r["relationship_id"] == "Maps to":
            assert by_id[r["concept_id_2"]]["standard_concept"] == "S", "Maps to must point at a standard concept"
    codes = [(c["vocabulary_id"], c["concept_code"]) for c in v["CONCEPT"]]
    assert len(codes) == len(set(codes))


def test_etl_never_hardcodes_concept_ids(project_root):
    """Concepts are always looked up by (vocabulary_id, concept_code) so a real Athena download works."""
    pattern = re.compile(r"\b2_?0{3}_?0{3}_?\d{3}\b|\b20000000\d\d\b")
    for folder in ("etl", "dq", "study", "mapping_assist", "site"):
        for f in (project_root / "src" / "omop_etl" / folder).rglob("*.py"):
            assert not pattern.search(f.read_text()), f"hard-coded concept id in {f}"


def _digest(folder):
    return {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(folder.iterdir())}


def test_generators_are_deterministic(tmp_path):
    clarity.generate(tmp_path / "a" / "CVH", n_patients=80)
    clarity.generate(tmp_path / "b" / "CVH", n_patients=80)
    assert _digest(tmp_path / "a" / "CVH") == _digest(tmp_path / "b" / "CVH")


def test_clarity_reproduces_real_quirks(pipeline):
    raw = pipeline["paths"].raw / "CVH"
    flo = (raw / "IP_FLWSHT_MEAS.txt").read_text()
    assert re.search(r"\|5\|\d+/\d+\|", flo), "blood pressure stored as one 'sys/dia' string"
    assert "|9999999|" in (raw / "ORDER_RESULTS.txt").read_text(), "non-numeric results flagged 9999999"
    assert "N18.3\n" in (raw / "EDG_CURRENT_ICD10.txt").read_text(), "retired ICD-10-CM code present"


def test_fhir_export_is_ndjson_with_manifest(pipeline):
    raw = pipeline["paths"].raw / "RCH"
    m = verify_manifest(raw)
    assert {f["file"] for f in m["files"]} >= {"Patient.ndjson", "Observation.ndjson", "Condition.ndjson"}
    first = json.loads((raw / "Observation.ndjson").read_text().splitlines()[0])
    assert first["resourceType"] == "Observation"


def test_manifest_detects_tampering(tmp_path):
    fhir.generate(tmp_path / "RCH", n_patients=20)
    with open(tmp_path / "RCH" / "Patient.ndjson", "a") as f:
        f.write('{"resourceType":"Patient","id":"intruder"}\n')
    with pytest.raises(ManifestError, match="checksum"):
        verify_manifest(tmp_path / "RCH")


def test_landing_to_s3_round_trip(tmp_path, monkeypatch):
    moto = pytest.importorskip("moto")
    import boto3

    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    fhir.generate(tmp_path / "raw" / "RCH", n_patients=20)
    with moto.mock_aws():
        boto3.client("s3").create_bucket(Bucket="landing-zone")
        m = land(tmp_path / "raw" / "RCH", "s3://landing-zone/partner-drops", tmp_path / "landing" / "RCH")
        keys = [o["Key"] for o in boto3.client("s3").list_objects_v2(Bucket="landing-zone")["Contents"]]
    assert "partner-drops/RCH/manifest.json" in keys
    assert len(m["files"]) == 7
