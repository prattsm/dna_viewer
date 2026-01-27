from __future__ import annotations

import hashlib
import logging
import time
from pathlib import Path
from typing import Callable

from dna_insights.core.db import Database
from dna_insights.core.insight_engine import build_qc_result, evaluate_modules
from dna_insights.core.knowledge_base import curated_rsids
from dna_insights.core.models import ImportSummary, KnowledgeModule, QCReport

from dna_insights.core.parser import (
    PARSER_VERSION,
    ancestry_text_total_bytes,
    close_ancestry_handle,
    open_ancestry_file,
    parse_ancestry_handle,
)
from dna_insights.core.security import EncryptionManager
from dna_insights.core.utils import safe_uuid, utc_now_iso


def _write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def _hash_and_store_raw(
    *,
    file_path: Path,
    raw_path: Path,
    encryption: EncryptionManager | None,
    on_progress_detail: Callable[[int, int, float], None] | None,
) -> str:
    try:
        total_bytes = int(file_path.stat().st_size)
    except FileNotFoundError:
        total_bytes = 0

    hasher = hashlib.sha256()
    bytes_read = 0
    last_emit = 0
    start_time = time.monotonic()

    def maybe_emit() -> None:
        if not on_progress_detail or total_bytes <= 0:
            return
        nonlocal last_emit
        if bytes_read - last_emit < 256 * 1024 and bytes_read < total_bytes:
            return
        last_emit = bytes_read
        elapsed = max(time.monotonic() - start_time, 0.001)
        rate = bytes_read / elapsed
        percent = min(int((bytes_read / total_bytes) * 100), 100)
        remaining = max(total_bytes - bytes_read, 0)
        eta_seconds = remaining / rate if rate > 0 else 0.0
        on_progress_detail(percent, bytes_read, eta_seconds)

    if encryption and encryption.is_enabled():
        if not encryption.has_key():
            raise RuntimeError("Encryption is enabled but passphrase has not been provided.")
        buffer = bytearray()
        with file_path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                hasher.update(chunk)
                buffer.extend(chunk)
                bytes_read += len(chunk)
                maybe_emit()
        encrypted = encryption.encrypt_bytes(bytes(buffer))
        _write_bytes(raw_path, encrypted)
    else:
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        with file_path.open("rb") as src, raw_path.open("wb") as dst:
            for chunk in iter(lambda: src.read(1024 * 1024), b""):
                hasher.update(chunk)
                dst.write(chunk)
                bytes_read += len(chunk)
                maybe_emit()

    if on_progress_detail and total_bytes > 0:
        on_progress_detail(100, bytes_read, 0.0)
    return hasher.hexdigest()


def _format_import_error(exc: Exception) -> str:
    message = str(exc).strip()
    if not message:
        message = exc.__class__.__name__
    return message[:500]


def import_ancestry_file(
    *,
    profile_id: str,
    file_path: Path,
    db_path: Path,
    modules: list[KnowledgeModule],
    kb_version: str,
    opt_in_categories: dict[str, bool],
    mode: str = "curated",
    zip_member: str | None = None,
    encryption: EncryptionManager | None = None,
    on_progress: Callable[[int], None] | None = None,
    on_stage: Callable[[str], None] | None = None,
    on_progress_detail: Callable[[int, int, float], None] | None = None,
) -> ImportSummary:
    if mode not in {"curated", "full"}:
        raise ValueError("mode must be 'curated' or 'full'")

    curated_set = curated_rsids(modules)

    db = Database(db_path)
    import_id: str | None = safe_uuid()
    imported_at = utc_now_iso()

    try:
        if on_stage:
            on_stage("Preparing raw file...")

        raw_suffix = ".enc" if encryption and encryption.is_enabled() else file_path.suffix
        raw_path = db_path.parent / "raw" / f"{import_id}{raw_suffix}"
        prep_start = time.monotonic()
        file_hash = _hash_and_store_raw(
            file_path=file_path,
            raw_path=raw_path,
            encryption=encryption,
            on_progress_detail=on_progress_detail,
        )
        prep_duration = max(time.monotonic() - prep_start, 0.001)
        try:
            file_size = file_path.stat().st_size
        except FileNotFoundError:
            file_size = 0
        logging.info(
            "Prepared raw file in %.2fs (%.1f MB).",
            prep_duration,
            file_size / (1024 * 1024),
        )

        import_id, imported_at = db.add_import(
            profile_id=profile_id,
            source="ancestry",
            file_hash_sha256=file_hash,
            parser_version=PARSER_VERSION,
            build="GRCh37",
            strand="+",
            imported_at=imported_at,
            status="running",
            zip_member=zip_member,
            import_id=import_id,
        )

        if on_stage:
            on_stage("Parsing raw data...")

        curated_rows: list[tuple] = []
        full_rows: list[tuple] = []
        curated_map: dict[str, dict] = {}

        curated_batch = 2000
        full_batch = 20000

        def on_record(record):
            if record.rsid in curated_set:
                curated_rows.append((profile_id, record.rsid, record.chrom, record.pos, record.genotype))
                curated_map[record.rsid] = {
                    "rsid": record.rsid,
                    "chrom": record.chrom,
                    "pos": record.pos,
                    "genotype": record.genotype,
                }
                if len(curated_rows) >= curated_batch:
                    db.insert_genotypes_curated(curated_rows)
                    curated_rows.clear()

            if mode == "full":
                full_rows.append((profile_id, record.rsid, record.chrom, record.pos, record.genotype))
                if len(full_rows) >= full_batch:
                    db.insert_genotypes_full(full_rows)
                    full_rows.clear()

        total_bytes = ancestry_text_total_bytes(file_path, member=zip_member)
        bytes_state = {"last_emit": 0}
        start_time = time.monotonic()

        def on_bytes(bytes_read: int) -> None:
            if not on_progress_detail or total_bytes <= 0:
                return
            now = time.monotonic()
            elapsed = max(now - start_time, 0.001)
            rate = bytes_read / elapsed
            if bytes_read - bytes_state["last_emit"] < 256 * 1024 and bytes_read < total_bytes:
                return
            bytes_state["last_emit"] = bytes_read
            percent = min(int((bytes_read / total_bytes) * 100), 100)
            remaining = max(total_bytes - bytes_read, 0)
            eta_seconds = remaining / rate if rate > 0 else 0.0
            on_progress_detail(percent, bytes_read, eta_seconds)

        handle = open_ancestry_file(file_path, member=zip_member)
        try:
            logging.info("Import starting: mode=%s zip_member=%s", mode, zip_member or "")
            db.begin()
            parse_start = time.monotonic()
            stats = parse_ancestry_handle(
                handle,
                on_record=on_record,
                on_progress=on_progress,
                on_bytes=on_bytes,
            )
            parse_duration = max(time.monotonic() - parse_start, 0.001)
            logging.info(
                "Parsed %s markers in %.2fs (%.0f markers/sec).",
                stats.total_markers,
                parse_duration,
                stats.total_markers / parse_duration,
            )

            if on_stage:
                on_stage("Writing genotypes...")
            if curated_rows:
                db.insert_genotypes_curated(curated_rows)
            if full_rows:
                db.insert_genotypes_full(full_rows)
            db.commit()
        except Exception:
            try:
                db.rollback()
            except Exception:
                pass
            raise
        finally:
            close_ancestry_handle(handle)

        qc = QCReport(
            total_markers=stats.total_markers,
            missing_calls=stats.missing_calls,
            call_rate=stats.call_rate(),
            duplicates=stats.duplicates,
            malformed_rows=stats.malformed_rows,
            sex_check=stats.sex_check(),
            warnings=stats.warnings,
        )

        if on_stage:
            on_stage("Generating insights...")
        insights_start = time.monotonic()
        insight_results = evaluate_modules(curated_map, modules, opt_in_categories)
        insight_results.append(build_qc_result(qc))
        db.store_insight_results(profile_id, insight_results, kb_version)
        logging.info("Insights generated in %.2fs.", max(time.monotonic() - insights_start, 0.001))

        db.update_import_status(import_id, status="ok", error_message=None)

        summary = ImportSummary(
            import_id=import_id,
            profile_id=profile_id,
            source="ancestry",
            file_hash_sha256=file_hash,
            imported_at=imported_at,
            parser_version=PARSER_VERSION,
            build="GRCh37",
            strand="+",
            qc_report=qc,
            insight_count=len(insight_results),
            kb_version=kb_version,
            curated_mode=True,
            full_mode=mode == "full",
        )

        return summary
    except Exception as exc:
        if import_id is not None:
            try:
                db.update_import_status(import_id, status="failed", error_message=_format_import_error(exc))
            except Exception:
                pass
        raise
    finally:
        db.close()
