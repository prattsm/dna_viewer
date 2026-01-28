from __future__ import annotations

import gzip
import hashlib
import io
import sqlite3
import time
from importlib import resources
from pathlib import Path
from typing import Callable

from dna_insights.core.db import Database
from dna_insights.core.exceptions import ImportCancelled
from dna_insights.core.utils import normalize_chrom, sha256_file

HIGH_CONFIDENCE_REVSTAT = {"practice_guideline", "reviewed_by_expert_panel"}
PATHOGENIC_LABELS = {"pathogenic", "likely_pathogenic"}
SEED_FILENAME = "clinvar_seed.tsv"
AUTO_IMPORT_NAMES = [
    "variant_summary.txt.gz",
    "variant_summary.txt",
    "clinvar.vcf.gz",
    "clinvar.vcf",
]
CLINVAR_CACHE_FILENAME = "clinvar_cache.sqlite3"
BATCH_SIZE = 5000


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


def _normalize_header_key(value: str) -> str:
    return "".join(value.strip().lower().split())


def _column_index(header: list[str], candidates: list[str]) -> int | None:
    header_map = {_normalize_header_key(name): idx for idx, name in enumerate(header)}
    for name in candidates:
        key = _normalize_header_key(name)
        if key in header_map:
            return header_map[key]
    return None


def _read_variant_summary_header(path: Path) -> list[str] | None:
    handle = _open_text(path)
    try:
        for line in handle:
            if not line.strip():
                continue
            if line.startswith("#"):
                if "AlleleID" in line:
                    return line.lstrip("#").rstrip("\n").split("\t")
                continue
            return line.rstrip("\n").split("\t")
    finally:
        _close_text(handle)
    return None


def _has_required_columns(header: list[str]) -> bool:
    rs_idx = _column_index(header, ["RS# (dbSNP)", "RS#(dbSNP)", "RS#"])
    clnsig_idx = _column_index(header, ["ClinicalSignificance"])
    review_idx = _column_index(header, ["ReviewStatus"])
    return rs_idx is not None and clnsig_idx is not None and review_idx is not None


def _field_at(line: str, index: int) -> str:
    if index < 0:
        return ""
    parts = line.rstrip("\n").split("\t", index + 1)
    if len(parts) <= index:
        return ""
    return parts[index]


def _is_variant_summary(path: Path) -> bool:
    name = path.name.lower()
    if "variant_summary" in name:
        return True
    if not (name.endswith(".txt") or name.endswith(".txt.gz")):
        return False
    header = _read_variant_summary_header(path)
    if not header or not _has_required_columns(header):
        raise ValueError("File does not look like ClinVar variant_summary.")
    return True


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
                    bytes_read += len(line)
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

        max_index = max(
            idx
            for idx in (
                rs_idx,
                clnsig_idx,
                review_idx,
                assembly_idx,
                chrom_idx,
                pos_idx,
                ref_idx,
                alt_idx,
                phenotype_idx,
                last_eval_idx,
            )
            if idx is not None
        )

        for line in handle:
            if cancel_check and cancel_check():
                raise ImportCancelled("ClinVar import cancelled.")
            if on_progress_detail and total_bytes > 0:
                if file_path.suffix.lower() == ".gz":
                    bytes_read = _compressed_bytes_read(handle)
                else:
                    bytes_read += len(line)
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
            rs_value = _field_at(line, rs_idx).strip()
            if not rs_value or rs_value == "-1":
                continue
            rsid = rs_value if rs_value.startswith("rs") else f"rs{rs_value}"
            if rsid_filter is not None and rsid not in rsid_filter:
                continue

            parts = line.rstrip("\n").split("\t", max_index + 1)
            if rs_idx >= len(parts):
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

            yield (rsid, normalize_chrom(chrom), pos, ref, alt, clnsig, review, conditions, last_eval)
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


def cache_path(data_dir: Path) -> Path:
    return data_dir / "clinvar" / CLINVAR_CACHE_FILENAME


def auto_import_source(data_dir: Path) -> dict | None:
    cache = cache_path(data_dir)
    if cache.exists():
        return {"kind": "cache", "path": cache}
    packaged = packaged_clinvar_path()
    if packaged:
        return {"kind": "file", "path": packaged}
    clinvar_dir = data_dir / "clinvar"
    for name in AUTO_IMPORT_NAMES:
        candidate = clinvar_dir / name
        if candidate.exists():
            return {"kind": "file", "path": candidate}
    return None


def build_clinvar_cache(
    *,
    input_path: Path,
    output_path: Path,
    on_progress_detail: Callable[[int, int, float], None] | None = None,
    cancel_check: Callable[[], bool] | None = None,
) -> dict:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(output_path, timeout=30)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS clinvar_variants (
            rsid TEXT PRIMARY KEY,
            chrom TEXT NOT NULL,
            pos INTEGER NOT NULL,
            ref TEXT NOT NULL,
            alt TEXT NOT NULL,
            clinical_significance TEXT,
            review_status TEXT,
            conditions TEXT,
            last_evaluated TEXT
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS clinvar_cache_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """
    )

    processed = 0
    batch: list[tuple] = []
    total_bytes = _total_bytes(input_path)
    start_time = time.monotonic()
    last_emit = 0
    bytes_read = 0

    def maybe_emit_progress() -> None:
        if not on_progress_detail or total_bytes <= 0:
            return
        nonlocal last_emit
        if bytes_read - last_emit < 512 * 1024 and bytes_read < total_bytes:
            return
        last_emit = bytes_read
        elapsed = max(time.monotonic() - start_time, 0.001)
        rate = bytes_read / elapsed
        percent = min(int((bytes_read / total_bytes) * 100), 100)
        remaining = max(total_bytes - bytes_read, 0)
        eta_seconds = remaining / rate if rate > 0 else 0.0
        on_progress_detail(percent, bytes_read, eta_seconds)

    try:
        conn.execute("BEGIN")
        if _is_variant_summary(input_path):
            for row in _iter_variant_summary(
                file_path=input_path,
                rsid_filter=None,
                on_progress_detail=on_progress_detail,
                cancel_check=cancel_check,
            ):
                if cancel_check and cancel_check():
                    raise ImportCancelled("ClinVar cache build cancelled.")
                batch.append(row)
                processed += 1
                if len(batch) >= BATCH_SIZE:
                    conn.executemany(
                        """
                        INSERT OR REPLACE INTO clinvar_variants
                            (rsid, chrom, pos, ref, alt, clinical_significance, review_status, conditions, last_evaluated)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        batch,
                    )
                    batch.clear()
        else:
            handle = _open_vcf(input_path)
            try:
                for line in handle:
                    if cancel_check and cancel_check():
                        raise ImportCancelled("ClinVar cache build cancelled.")
                    lower = line.lower()
                    if line.startswith("##"):
                        if "grch38" in lower or "hg38" in lower:
                            raise ValueError("ClinVar VCF appears to be GRCh38; expected GRCh37.")
                        continue
                    if input_path.suffix.lower() == ".gz":
                        bytes_read = _compressed_bytes_read(handle)
                    else:
                        bytes_read += len(line)
                    maybe_emit_progress()
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
                    conditions = info_map.get("CLNDN") or info_map.get("CLNDISDB") or ""
                    last_eval = info_map.get("CLNDATE", "")
                    batch.append(
                        (
                            rsid,
                            normalize_chrom(chrom),
                            int(pos),
                            ref,
                            alt,
                            clnsig,
                            review,
                            conditions,
                            last_eval,
                        )
                    )
                    processed += 1
                    if len(batch) >= BATCH_SIZE:
                        conn.executemany(
                            """
                            INSERT OR REPLACE INTO clinvar_variants
                                (rsid, chrom, pos, ref, alt, clinical_significance, review_status, conditions, last_evaluated)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            batch,
                        )
                        batch.clear()
            finally:
                _close_text(handle)

        if batch:
            conn.executemany(
                """
                INSERT OR REPLACE INTO clinvar_variants
                    (rsid, chrom, pos, ref, alt, clinical_significance, review_status, conditions, last_evaluated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                batch,
            )

        count = int(conn.execute("SELECT COUNT(*) FROM clinvar_variants").fetchone()[0])
        file_hash = sha256_file(input_path)
        conn.execute("DELETE FROM clinvar_cache_meta")
        conn.executemany(
            "INSERT INTO clinvar_cache_meta (key, value) VALUES (?, ?)",
            [
                ("file_hash_sha256", file_hash),
                ("variant_count", str(count)),
                ("source_path", str(input_path)),
            ],
        )
        conn.commit()
        return {"file_hash_sha256": file_hash, "variant_count": count, "processed": processed}
    except ImportCancelled:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def import_clinvar_cache(
    *,
    cache_path: Path,
    db_path: Path,
    rsid_filter: set[str],
    on_progress: Callable[[int], None] | None = None,
    on_progress_detail: Callable[[int, int, float], None] | None = None,
    replace: bool = True,
    cancel_check: Callable[[], bool] | None = None,
) -> dict:
    if not cache_path.exists():
        raise FileNotFoundError(f"ClinVar cache not found at {cache_path}")

    db = Database(db_path)
    conn = db.conn
    attached = False
    progress_handler_set = False
    try:
        conn.execute("ATTACH DATABASE ? AS clinvar_cache", (str(cache_path),))
        attached = True

        file_hash = None
        try:
            row = conn.execute(
                "SELECT value FROM clinvar_cache.clinvar_cache_meta WHERE key = ?",
                ("file_hash_sha256",),
            ).fetchone()
            if row:
                file_hash = row[0]
        except sqlite3.Error:
            file_hash = None
        if not file_hash:
            file_hash = sha256_file(cache_path)

        latest = db.get_latest_clinvar_import()
        if latest and latest.get("file_hash_sha256") != file_hash:
            replace = True
            if rsid_filter is not None:
                rsid_filter = db.get_all_rsids()

        if not rsid_filter:
            return {"skipped": True, "reason": "no_rsids"}

        rsids = list(rsid_filter)
        total = len(rsids)
        if total == 0:
            return {"skipped": True, "reason": "no_rsids"}

        def emit_detail(percent: int, processed: int, eta_seconds: float) -> None:
            if on_progress_detail:
                on_progress_detail(percent, processed, eta_seconds)

        def emit_progress(count: int) -> None:
            if on_progress:
                on_progress(count)

        def progress_handler() -> int:
            if cancel_check and cancel_check():
                return 1
            return 0

        try:
            if cancel_check:
                conn.set_progress_handler(progress_handler, 10000)
                progress_handler_set = True

            if replace:
                db.clear_clinvar_variants(commit=True)
                db.clear_clinvar_checked(commit=True)

            conn.execute("DROP TABLE IF EXISTS temp.rsid_input")
            conn.execute("DROP TABLE IF EXISTS temp.missing_rsids")
            conn.execute("CREATE TEMP TABLE rsid_input (rsid TEXT PRIMARY KEY)")
            conn.commit()

            inserted = 0
            insert_start = time.monotonic()
            chunk_size = 10000
            for i in range(0, total, chunk_size):
                if cancel_check and cancel_check():
                    raise ImportCancelled("ClinVar cache import cancelled.")
                chunk = rsids[i : i + chunk_size]
                rows = [(rsid,) for rsid in chunk]
                conn.executemany("INSERT OR IGNORE INTO rsid_input (rsid) VALUES (?)", rows)
                conn.commit()
                inserted += len(chunk)
                if total > 0:
                    elapsed = max(time.monotonic() - insert_start, 0.001)
                    rate = inserted / elapsed
                    remaining = max(total - inserted, 0)
                    eta_seconds = remaining / rate if rate > 0 else 0.0
                    percent = min(int((inserted / total) * 15), 15)
                    emit_detail(percent, inserted, eta_seconds)

            if cancel_check and cancel_check():
                raise ImportCancelled("ClinVar cache import cancelled.")

            conn.execute(
                """
                CREATE TEMP TABLE missing_rsids AS
                SELECT r.rsid
                FROM rsid_input r
                LEFT JOIN clinvar_checked c ON c.rsid = r.rsid
                WHERE c.rsid IS NULL
                """
            )
            conn.commit()

            row = conn.execute(
                "SELECT COUNT(*) AS total, MIN(rowid) AS min_id, MAX(rowid) AS max_id FROM missing_rsids"
            ).fetchone()
            total_missing = int(row["total"]) if row else 0
            min_rowid = row["min_id"] if row else None
            max_rowid = row["max_id"] if row else None

            if total_missing == 0 or min_rowid is None or max_rowid is None:
                db.rollback()
                return {"skipped": True, "reason": "already_checked"}

            emit_detail(20, 0, 0.0)

            matched_total = int(
                conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM missing_rsids m
                    JOIN clinvar_cache.clinvar_variants c ON c.rsid = m.rsid
                    """
                ).fetchone()[0]
            )

            processed_missing = 0
            import_start = time.monotonic()
            step_size = 50000
            for start_id in range(int(min_rowid), int(max_rowid) + 1, step_size):
                if cancel_check and cancel_check():
                    raise ImportCancelled("ClinVar cache import cancelled.")
                end_id = min(start_id + step_size - 1, int(max_rowid))
                chunk_total = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM missing_rsids WHERE rowid BETWEEN ? AND ?",
                        (start_id, end_id),
                    ).fetchone()[0]
                )
                if chunk_total == 0:
                    continue

                try:
                    conn.execute("BEGIN")
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO clinvar_variants
                            (rsid, chrom, pos, ref, alt, clinical_significance, review_status, conditions, last_evaluated)
                        SELECT c.rsid, c.chrom, c.pos, c.ref, c.alt, c.clinical_significance, c.review_status,
                               c.conditions, c.last_evaluated
                        FROM missing_rsids m
                        JOIN clinvar_cache.clinvar_variants c ON c.rsid = m.rsid
                        WHERE m.rowid BETWEEN ? AND ?
                        """,
                        (start_id, end_id),
                    )
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO clinvar_checked (rsid)
                        SELECT rsid FROM missing_rsids WHERE rowid BETWEEN ? AND ?
                        """,
                        (start_id, end_id),
                    )
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise

                processed_missing += chunk_total
                elapsed = max(time.monotonic() - import_start, 0.001)
                rate = processed_missing / elapsed
                remaining = max(total_missing - processed_missing, 0)
                eta_seconds = remaining / rate if rate > 0 else 0.0
                percent = 20 + int((processed_missing / total_missing) * 75)
                emit_detail(min(percent, 95), processed_missing, eta_seconds)

                if matched_total > 0:
                    estimate = int(matched_total * (processed_missing / total_missing))
                    emit_progress(min(estimate, matched_total))
                else:
                    emit_progress(processed_missing)

            db.add_clinvar_import(file_hash, matched_total, commit=True)
        except sqlite3.OperationalError as exc:
            if cancel_check and cancel_check() and "interrupted" in str(exc).lower():
                try:
                    db.rollback()
                except Exception:
                    pass
                raise ImportCancelled("ClinVar cache import cancelled.") from exc
            try:
                db.rollback()
            except Exception:
                pass
            raise
        except ImportCancelled:
            try:
                db.rollback()
            except Exception:
                pass
            raise
        except Exception:
            try:
                db.rollback()
            except Exception:
                pass
            raise
        finally:
            if progress_handler_set:
                conn.set_progress_handler(None, 0)

        emit_detail(100, total_missing, 0.0)
        emit_progress(matched_total)
        return {"file_hash_sha256": file_hash, "variant_count": matched_total}
    finally:
        if attached:
            try:
                conn.execute("DETACH DATABASE clinvar_cache")
            except Exception:
                pass
        db.close()


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
    try:
        file_hash = sha256_file(file_path)
        latest = db.get_latest_clinvar_import()
        if latest and latest.get("file_hash_sha256") != file_hash:
            replace = True
            if rsid_filter is not None:
                rsid_filter = db.get_all_rsids()
        if latest and latest.get("file_hash_sha256") == file_hash and rsid_filter is None:
            return {"skipped": True, "reason": "already_imported", **latest}
        if rsid_filter is not None and not rsid_filter:
            return {"skipped": True, "reason": "no_rsids"}

        processed = 0
        unique_rsids: set[str] = set()
        batch: list[tuple] = []
        total_bytes = _total_bytes(file_path)

        try:
            if replace:
                db.clear_clinvar_variants(commit=True)
                db.clear_clinvar_checked(commit=True)

            if _is_variant_summary(file_path):
                for row in _iter_variant_summary(
                    file_path=file_path,
                    rsid_filter=rsid_filter,
                    on_progress_detail=on_progress_detail,
                    cancel_check=cancel_check,
                ):
                    batch.append(row)
                    processed += 1
                    rsid = row[0]
                    if rsid not in unique_rsids:
                        unique_rsids.add(rsid)

                    if len(batch) >= BATCH_SIZE:
                        if cancel_check and cancel_check():
                            raise ImportCancelled("ClinVar import cancelled.")
                        db.upsert_clinvar_variants(batch)
                        db.commit()
                        batch.clear()

                    if on_progress and processed % 5000 == 0:
                        on_progress(processed)
            else:
                handle = _open_vcf(file_path)
                bytes_read = 0
                last_emit = 0
                start_time = time.monotonic()
                try:
                    for line in handle:
                        if cancel_check and cancel_check():
                            raise ImportCancelled("ClinVar import cancelled.")
                        lower = line.lower()
                        if line.startswith("##"):
                            if "grch38" in lower or "hg38" in lower:
                                raise ValueError("ClinVar VCF appears to be GRCh38; expected GRCh37.")
                            continue
                        if on_progress_detail and total_bytes > 0:
                            if file_path.suffix.lower() == ".gz":
                                bytes_read = _compressed_bytes_read(handle)
                            else:
                                bytes_read += len(line)
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
                                normalize_chrom(chrom),
                                int(pos),
                                ref,
                                alt,
                                clnsig,
                                review,
                                conditions,
                                last_eval,
                            )
                        )
                        processed += 1
                        if rsid not in unique_rsids:
                            unique_rsids.add(rsid)

                        if len(batch) >= BATCH_SIZE:
                            if cancel_check and cancel_check():
                                raise ImportCancelled("ClinVar import cancelled.")
                            db.upsert_clinvar_variants(batch)
                            db.commit()
                            batch.clear()

                        if on_progress and processed % 5000 == 0:
                            on_progress(processed)
                finally:
                    _close_text(handle)

            if batch:
                if cancel_check and cancel_check():
                    raise ImportCancelled("ClinVar import cancelled.")
                db.upsert_clinvar_variants(batch)
                db.commit()

            if rsid_filter is not None:
                db.mark_clinvar_checked(rsid_filter, commit=True)
            db.add_clinvar_import(file_hash, len(unique_rsids), commit=True)
        except ImportCancelled:
            try:
                db.rollback()
            except Exception:
                pass
            raise
        except Exception:
            try:
                db.rollback()
            except Exception:
                pass
            raise

        return {
            "file_hash_sha256": file_hash,
            "variant_count": len(unique_rsids),
        }
    finally:
        db.close()
