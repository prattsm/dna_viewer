from __future__ import annotations

import gzip
import hashlib
from importlib import resources
from pathlib import Path
from typing import Callable

from dna_insights.core.db import Database
from dna_insights.core.utils import sha256_file

HIGH_CONFIDENCE_REVSTAT = {"practice_guideline", "reviewed_by_expert_panel"}
PATHOGENIC_LABELS = {"pathogenic", "likely_pathogenic"}
SEED_FILENAME = "clinvar_seed.tsv"
AUTO_IMPORT_NAMES = ["clinvar.vcf.gz", "clinvar.vcf"]


def _open_vcf(path: Path):
    if path.suffix.lower() == ".gz":
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return path.open("r", encoding="utf-8", errors="replace")


def _parse_info(info: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for item in info.split(";"):
        if not item:
            continue
        if "=" not in item:
            result[item] = ""
            continue
        key, value = item.split("=", 1)
        result[key] = value
    return result


def _split_values(value: str) -> list[str]:
    if not value:
        return []
    for sep in ("|", ","):
        if sep in value:
            return [part.strip() for part in value.split(sep) if part.strip()]
    return [value.strip()]


def _is_high_confidence(review_status: str) -> bool:
    review_lower = review_status.lower()
    return any(token in review_lower for token in HIGH_CONFIDENCE_REVSTAT)


def _is_pathogenic(cln_sig: str) -> bool:
    values = {value.lower() for value in _split_values(cln_sig)}
    if "conflicting_interpretations_of_pathogenicity" in values:
        return False
    return bool(values & PATHOGENIC_LABELS)


def _seed_bytes() -> bytes:
    seed_path = resources.files("dna_insights.knowledge_base") / SEED_FILENAME
    return seed_path.read_bytes()


def seed_metadata() -> dict:
    data = _seed_bytes()
    lines = [line for line in data.decode("utf-8").splitlines() if line.strip()]
    variant_count = max(len(lines) - 1, 0)
    return {
        "file_hash_sha256": hashlib.sha256(data).hexdigest(),
        "variant_count": variant_count,
    }


def _parse_seed_variants(text: str) -> list[tuple]:
    rows: list[tuple] = []
    for line in text.splitlines():
        if not line.strip():
            continue
        if line.startswith("#") or line.lower().startswith("rsid"):
            continue
        parts = line.split("\t")
        if len(parts) < 9:
            continue
        rsid, chrom, pos, ref, alt, clnsig, review, conditions, last_eval = parts[:9]
        try:
            pos_int = int(pos)
        except ValueError:
            continue
        rows.append((rsid, chrom, pos_int, ref, alt, clnsig, review, conditions, last_eval))
    return rows


def seed_clinvar_if_missing(db: Database) -> dict:
    if db.get_latest_clinvar_import():
        return {"seeded": False}
    data = _seed_bytes()
    rows = _parse_seed_variants(data.decode("utf-8"))
    if not rows:
        return {"seeded": False}
    db.upsert_clinvar_variants(rows)
    db.commit()
    meta = seed_metadata()
    db.add_clinvar_import(meta["file_hash_sha256"], meta["variant_count"])
    return {"seeded": True, **meta}


def auto_import_path(data_dir: Path) -> Path | None:
    clinvar_dir = data_dir / "clinvar"
    for name in AUTO_IMPORT_NAMES:
        candidate = clinvar_dir / name
        if candidate.exists():
            return candidate
    return None


def import_clinvar_snapshot(
    *,
    file_path: Path,
    db_path: Path,
    on_progress: Callable[[int], None] | None = None,
    replace: bool = True,
    rsid_filter: set[str] | None = None,
) -> dict:
    db = Database(db_path)
    if rsid_filter is not None and not rsid_filter:
        db.close()
        return {"skipped": True, "reason": "no_rsids"}

    file_hash = sha256_file(file_path)
    latest = db.get_latest_clinvar_import()
    if latest and latest.get("file_hash_sha256") == file_hash:
        db.close()
        return {"skipped": True, "reason": "already_imported", **latest}

    if replace:
        db.clear_clinvar_variants()
    inserted = 0
    batch: list[tuple] = []

    handle = _open_vcf(file_path)
    try:
        for line_number, line in enumerate(handle, start=1):
            if line.startswith("#"):
                continue
            parts = line.strip().split("\t")
            if len(parts) < 8:
                continue
            chrom, pos, rsid, ref, alt, _qual, _filter, info = parts[:8]
            if not rsid.startswith("rs"):
                continue
            info_map = _parse_info(info)
            clnsig = info_map.get("CLNSIG", "")
            review = info_map.get("CLNREVSTAT", "")
            if not (_is_high_confidence(review) and _is_pathogenic(clnsig)):
                continue
            if rsid_filter is not None and rsid not in rsid_filter:
                continue

            conditions = info_map.get("CLNDN") or info_map.get("CLNDISDB") or ""
            last_eval = info_map.get("CLNDATE", "")

            batch.append(
                (
                    rsid,
                    chrom,
                    int(pos),
                    ref,
                    alt,
                    clnsig,
                    review,
                    conditions,
                    last_eval,
                )
            )
            inserted += 1

            if len(batch) >= 1000:
                db.upsert_clinvar_variants(batch)
                db.commit()
                batch.clear()

            if on_progress and inserted % 5000 == 0:
                on_progress(inserted)
    finally:
        handle.close()

    if batch:
        db.upsert_clinvar_variants(batch)
        db.commit()

    db.add_clinvar_import(file_hash, inserted)
    db.close()
    return {
        "file_hash_sha256": file_hash,
        "variant_count": inserted,
    }
