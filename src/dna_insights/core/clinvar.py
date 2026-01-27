from __future__ import annotations

import gzip
from pathlib import Path
from typing import Callable, Iterable

from dna_insights.core.db import Database
from dna_insights.core.utils import sha256_file

HIGH_CONFIDENCE_REVSTAT = {"practice_guideline", "reviewed_by_expert_panel"}
PATHOGENIC_LABELS = {"pathogenic", "likely_pathogenic"}


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


def import_clinvar_snapshot(
    *,
    file_path: Path,
    db_path: Path,
    on_progress: Callable[[int], None] | None = None,
) -> dict:
    file_hash = sha256_file(file_path)
    db = Database(db_path)
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
