from __future__ import annotations

import shutil
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
from dna_insights.core.utils import sha256_file, utc_now_iso


def _write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


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

    if on_stage:
        on_stage("Parsing raw data...")
    file_hash = sha256_file(file_path)
    curated_set = curated_rsids(modules)

    db = Database(db_path)
    import_id: str | None = None
    imported_at = utc_now_iso()

    try:
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
        )

        if encryption and encryption.is_enabled():
            if not encryption.has_key():
                raise RuntimeError("Encryption is enabled but passphrase has not been provided.")
            raw_bytes = file_path.read_bytes()
            encrypted = encryption.encrypt_bytes(raw_bytes)
            raw_path = db_path.parent / "raw" / f"{import_id}.enc"
            _write_bytes(raw_path, encrypted)
        else:
            raw_path = db_path.parent / "raw" / f"{import_id}{file_path.suffix}"
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(file_path, raw_path)

        curated_rows: list[tuple] = []
        full_rows: list[tuple] = []
        curated_map: dict[str, dict] = {}

        curated_batch = 1000
        full_batch = 5000

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
            db.begin()
            stats = parse_ancestry_handle(
                handle,
                on_record=on_record,
                on_progress=on_progress,
                on_bytes=on_bytes,
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
        insight_results = evaluate_modules(curated_map, modules, opt_in_categories)
        insight_results.append(build_qc_result(qc))
        db.store_insight_results(profile_id, insight_results, kb_version)

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
