from __future__ import annotations

import gzip
import hashlib
import io
import time
from importlib import resources
from pathlib import Path
from typing import Callable

from dna_insights.core.db import Database
from dna_insights.core.exceptions import ImportCancelled
from dna_insights.core.utils import sha256_file

HIGH_CONFIDENCE_REVSTAT = {"practice_guideline", "reviewed_by_expert_panel"}
PATHOGENIC_LABELS = {"pathogenic", "likely_pathogenic"}
SEED_FILENAME = "clinvar_seed.tsv"
AUTO_IMPORT_NAMES = [
    "variant_summary.txt.gz",
    "variant_summary.txt",
    "clinvar.vcf.gz",
    "clinvar.vcf",
]


def _open_vcf(path: Path):
    return _open_text(path)


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
    for sep in ("|", ",", ";", "/"):
        if sep in value:
            return [part.strip() for part in value.split(sep) if part.strip()]
    return [value.strip()]


def _is_high_confidence(review_status: str) -> bool:
    review_lower = review_status.lower().replace(" ", "_")
    return any(token in review_lower for token in HIGH_CONFIDENCE_REVSTAT)


def _is_pathogenic(cln_sig: str) -> bool:
    values = {value.lower() for value in _split_values(cln_sig)}
    if "conflicting_interpretations_of_pathogenicity" in values:
        return False
    return bool(values & PATHOGENIC_LABELS)


def classify_clinvar(clinical_significance: str, review_status: str) -> dict:
    values = {value.lower() for value in _split_values(clinical_significance)}
    review_lower = review_status.lower().replace(" ", "_")

    conflict = any("conflicting" in value for value in values)
    if "conflict" in review_lower and "no_conflicts" not in review_lower:
        conflict = True

    if _is_high_confidence(review_status):
        confidence = "High"
    elif "criteria_provided" in review_lower and "multiple_submitters" in review_lower and "no_conflicts" in review_lower:
        confidence = "Moderate"
    elif "criteria_provided" in review_lower or "single_submitter" in review_lower:
        confidence = "Low"
    elif "no_assertion" in review_lower:
        confidence = "Low"
    else:
        confidence = "Unknown"

    return {"confidence": confidence, "conflict": conflict}


def _open_text(path: Path):
    if path.suffix.lower() == ".gz":
        raw = path.open("rb")
        gz = gzip.GzipFile(fileobj=raw, mode="rb")
        text = io.TextIOWrapper(gz, encoding="utf-8", errors="replace")
        text._raw_file = raw  # type: ignore[attr-defined]
        text._gzip_handle = gz  # type: ignore[attr-defined]
        return text
    return path.open("r", encoding="utf-8", errors="replace")


def _close_text(handle: io.TextIOBase) -> None:
    raw = getattr(handle, "_raw_file", None)
    gz = getattr(handle, "_gzip_handle", None)
    try:
        handle.close()
    finally:
        if gz is not None:
            gz.close()
        if raw is not None:
            raw.close()


def _compressed_bytes_read(handle: io.TextIOBase) -> int:
    gz = getattr(handle, "_gzip_handle", None)
    if gz is None:
        return 0
    try:
        fileobj = getattr(gz, "fileobj", None)
        if fileobj is None:
            return 0
        return int(fileobj.tell())
    except Exception:
        return 0


def _total_bytes(path: Path) -> int:
    try:
        return int(path.stat().st_size)
    except FileNotFoundError:
        return 0


def _is_variant_summary(path: Path) -> bool:
    name = path.name.lower()
    if "variant_summary" in name:
        return True
    if name.endswith(".txt") or name.endswith(".txt.gz"):
        return True
    return False


def _column_index(header: list[str], candidates: list[str]) -> int | None:
    header_map = {name: idx for idx, name in enumerate(header)}
    for name in candidates:
        if name in header_map:
            return header_map[name]
    return None


def _iter_variant_summary(
    *,
    file_path: Path,
    rsid_filter: set[str] | None,
    on_progress_detail: Callable[[int, int, float], None] | None,
    cancel_check: Callable[[], bool] | None,
):
    handle = _open_text(file_path)
    total_bytes = _total_bytes(file_path)
    bytes_read = 0
    last_emit = 0
    start_time = time.monotonic()
    try:
        header: list[str] | None = None
        for line in handle:
            if cancel_check and cancel_check():
                raise ImportCancelled("ClinVar import cancelled.")
            if on_progress_detail and total_bytes > 0:
                if file_path.suffix.lower() == ".gz":
                    bytes_read = _compressed_bytes_read(handle)
                else:
                    bytes_read += len(line.encode("utf-8", errors="ignore"))
                if bytes_read - last_emit >= 512 * 1024 or bytes_read >= total_bytes:
                    last_emit = bytes_read
                    elapsed = max(time.monotonic() - start_time, 0.001)
                    rate = bytes_read / elapsed
                    percent = min(int((bytes_read / total_bytes) * 100), 100)
                    remaining = max(total_bytes - bytes_read, 0)
                    eta_seconds = remaining / rate if rate > 0 else 0.0
                    on_progress_detail(percent, bytes_read, eta_seconds)
            if not line.strip():
                continue
            if line.startswith("#"):
                if "AlleleID" in line:
                    header = line.lstrip("#").rstrip("\n").split("\t")
                    break
                continue
            header = line.rstrip("\n").split("\t")
            break
        if not header:
            return

        rs_idx = _column_index(header, ["RS# (dbSNP)", "RS#(dbSNP)", "RS#"])
        clnsig_idx = _column_index(header, ["ClinicalSignificance"])
        review_idx = _column_index(header, ["ReviewStatus"])
        assembly_idx = _column_index(header, ["Assembly"])
        chrom_idx = _column_index(header, ["Chromosome"])
        pos_idx = _column_index(header, ["PositionVCF", "Start"])
        ref_idx = _column_index(header, ["ReferenceAlleleVCF", "ReferenceAllele"])
        alt_idx = _column_index(header, ["AlternateAlleleVCF", "AlternateAllele"])
        phenotype_idx = _column_index(header, ["PhenotypeList"])
        last_eval_idx = _column_index(header, ["LastEvaluated"])

        if rs_idx is None or clnsig_idx is None or review_idx is None:
            raise ValueError("variant_summary is missing required columns.")

        for line in handle:
            if cancel_check and cancel_check():
                raise ImportCancelled("ClinVar import cancelled.")
            if on_progress_detail and total_bytes > 0:
                if file_path.suffix.lower() == ".gz":
                    bytes_read = _compressed_bytes_read(handle)
                else:
                    bytes_read += len(line.encode("utf-8", errors="ignore"))
                if bytes_read - last_emit >= 512 * 1024 or bytes_read >= total_bytes:
                    last_emit = bytes_read
                    elapsed = max(time.monotonic() - start_time, 0.001)
                    rate = bytes_read / elapsed
                    percent = min(int((bytes_read / total_bytes) * 100), 100)
                    remaining = max(total_bytes - bytes_read, 0)
                    eta_seconds = remaining / rate if rate > 0 else 0.0
                    on_progress_detail(percent, bytes_read, eta_seconds)
            if not line.strip():
                continue
            parts = line.rstrip("\n").split("\t")
            if rs_idx >= len(parts):
                continue
            rs_value = parts[rs_idx].strip()
            if not rs_value or rs_value == "-1":
                continue
            rsid = rs_value if rs_value.startswith("rs") else f"rs{rs_value}"
            if rsid_filter is not None and rsid not in rsid_filter:
                continue

            assembly = parts[assembly_idx].strip() if assembly_idx is not None and assembly_idx < len(parts) else ""
            if assembly and not assembly.upper().startswith("GRCH37"):
                continue

            clnsig = parts[clnsig_idx].strip() if clnsig_idx < len(parts) else ""
            review = parts[review_idx].strip() if review_idx < len(parts) else ""

            chrom = parts[chrom_idx].strip() if chrom_idx is not None and chrom_idx < len(parts) else ""
            pos_raw = parts[pos_idx].strip() if pos_idx is not None and pos_idx < len(parts) else ""
            try:
                pos = int(pos_raw)
            except ValueError:
                continue

            ref = parts[ref_idx].strip() if ref_idx is not None and ref_idx < len(parts) else ""
            alt = parts[alt_idx].strip() if alt_idx is not None and alt_idx < len(parts) else ""
            conditions = parts[phenotype_idx].strip() if phenotype_idx is not None and phenotype_idx < len(parts) else ""
            last_eval = parts[last_eval_idx].strip() if last_eval_idx is not None and last_eval_idx < len(parts) else ""

            yield (rsid, chrom, pos, ref, alt, clnsig, review, conditions, last_eval)
    finally:
        _close_text(handle)


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


def packaged_clinvar_path() -> Path | None:
    base = Path(__file__).resolve().parents[1] / "knowledge_base" / "clinvar_full"
    for name in ("variant_summary.txt.gz", "variant_summary.txt"):
        candidate = base / name
        if candidate.exists():
            return candidate
    return None


def auto_import_path(data_dir: Path) -> Path | None:
    packaged = packaged_clinvar_path()
    if packaged:
        return packaged
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
    on_progress_detail: Callable[[int, int, float], None] | None = None,
    replace: bool = True,
    rsid_filter: set[str] | None = None,
    cancel_check: Callable[[], bool] | None = None,
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

    inserted = 0
    batch: list[tuple] = []
    total_bytes = _total_bytes(file_path)

    try:
        db.begin()
        if replace:
            db.clear_clinvar_variants(commit=False)

        if _is_variant_summary(file_path):
            for row in _iter_variant_summary(
                file_path=file_path,
                rsid_filter=rsid_filter,
                on_progress_detail=on_progress_detail,
                cancel_check=cancel_check,
            ):
                batch.append(row)
                inserted += 1

                if len(batch) >= 1000:
                    db.upsert_clinvar_variants(batch)
                    batch.clear()

                if on_progress and inserted % 5000 == 0:
                    on_progress(inserted)
        else:
            handle = _open_vcf(file_path)
            bytes_read = 0
            last_emit = 0
            start_time = time.monotonic()
            try:
                for line in handle:
                    if cancel_check and cancel_check():
                        raise ImportCancelled("ClinVar import cancelled.")
                    if on_progress_detail and total_bytes > 0:
                        if file_path.suffix.lower() == ".gz":
                            bytes_read = _compressed_bytes_read(handle)
                        else:
                            bytes_read += len(line.encode("utf-8", errors="ignore"))
                        if bytes_read - last_emit >= 512 * 1024 or bytes_read >= total_bytes:
                            last_emit = bytes_read
                            elapsed = max(time.monotonic() - start_time, 0.001)
                            rate = bytes_read / elapsed
                            percent = min(int((bytes_read / total_bytes) * 100), 100)
                            remaining = max(total_bytes - bytes_read, 0)
                            eta_seconds = remaining / rate if rate > 0 else 0.0
                            on_progress_detail(percent, bytes_read, eta_seconds)
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
                        batch.clear()

                    if on_progress and inserted % 5000 == 0:
                        on_progress(inserted)
            finally:
                _close_text(handle)

        if batch:
            db.upsert_clinvar_variants(batch)

        db.commit()
    except ImportCancelled:
        try:
            db.rollback()
        except Exception:
            pass
        db.close()
        raise
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        db.close()
        raise

    db.add_clinvar_import(file_hash, inserted)
    db.close()
    return {
        "file_hash_sha256": file_hash,
        "variant_count": inserted,
    }
