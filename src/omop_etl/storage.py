"""Landing zone: where partner files arrive, local folder or Amazon S3.

Health systems drop their extract plus a manifest (file names, row counts, SHA-256 checksums). The
pipeline only processes a drop whose manifest checks out, so a half-uploaded or corrupted file can
never be loaded.

    OMOP_LANDING_URI unset            -> files are copied to data/landing/<SITE>/
    OMOP_LANDING_URI=s3://bucket/pfx  -> files are uploaded to S3, then pulled back down to
                                         data/landing/<SITE>/ for Spark (on EMR or Databricks Spark
                                         would read s3a:// paths directly instead)

boto3 is only imported for S3, so the default local run needs no AWS packages.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path


class ManifestError(Exception):
    pass


def verify_manifest(folder: Path) -> dict:
    manifest = json.loads((folder / "manifest.json").read_text())
    problems = []
    for f in manifest["files"]:
        path = folder / f["file"]
        if not path.exists():
            problems.append(f"{f['file']}: missing")
            continue
        data = path.read_bytes()
        if hashlib.sha256(data).hexdigest() != f["sha256"]:
            problems.append(f"{f['file']}: checksum mismatch")
        lines = data.count(b"\n") - (1 if path.suffix == ".txt" else 0)  # .txt files carry a header row
        if lines != f["rows"]:
            problems.append(f"{f['file']}: {lines} rows, manifest says {f['rows']}")
    if problems:
        raise ManifestError(f"{folder.name}: " + "; ".join(problems))
    return manifest


def _split_s3(uri: str) -> tuple[str, str]:
    bucket, _, prefix = uri.removeprefix("s3://").partition("/")
    return bucket, prefix.strip("/")


def land(raw_site_dir: Path, landing_uri: str | None, local_landing: Path) -> dict:
    """Deliver one site's drop to the landing zone and return the verified manifest."""
    verify_manifest(raw_site_dir)  # never ship a broken drop
    site = raw_site_dir.name
    if local_landing.exists():
        shutil.rmtree(local_landing)
    local_landing.mkdir(parents=True)

    if landing_uri and landing_uri.startswith("s3://"):
        import boto3

        s3 = boto3.client("s3")
        bucket, prefix = _split_s3(landing_uri)
        keys = []
        for path in sorted(raw_site_dir.iterdir()):
            key = f"{prefix}/{site}/{path.name}".lstrip("/")
            s3.upload_file(str(path), bucket, key)
            keys.append(key)
        for key in keys:
            s3.download_file(bucket, key, str(local_landing / Path(key).name))
    else:
        for path in raw_site_dir.iterdir():
            shutil.copy2(path, local_landing / path.name)
    return verify_manifest(local_landing)
