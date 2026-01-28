from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Iterable

from dna_insights.core.utils import safe_uuid, utc_now_iso


SCHEMA_VERSION = 4


class Database:
    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA journal_mode = WAL")
        self.conn.execute("PRAGMA synchronous = NORMAL")
        self.conn.execute("PRAGMA busy_timeout = 5000")
        self._migrate()

    def close(self) -> None:
        self.conn.close()

    def _migrate(self) -> None:
        cur = self.conn.execute("PRAGMA user_version")
        version = cur.fetchone()[0]
        if version < 1:
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS profiles (
                    id TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    notes TEXT,
                    created_at TEXT NOT NULL,
                    encryption_enabled INTEGER NOT NULL DEFAULT 1
                );

                CREATE TABLE IF NOT EXISTS imports (
                    id TEXT PRIMARY KEY,
                    profile_id TEXT NOT NULL,
                    source TEXT NOT NULL,
                    file_hash_sha256 TEXT NOT NULL,
                    imported_at TEXT NOT NULL,
                    parser_version TEXT NOT NULL,
                    build TEXT NOT NULL,
                    strand TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'ok',
                    error_message TEXT,
                    zip_member TEXT,
                    FOREIGN KEY(profile_id) REFERENCES profiles(id)
                );

                CREATE TABLE IF NOT EXISTS genotypes_curated (
                    profile_id TEXT NOT NULL,
                    rsid TEXT NOT NULL,
                    chrom TEXT NOT NULL,
                    pos INTEGER NOT NULL,
                    genotype TEXT,
                    PRIMARY KEY(profile_id, rsid)
                );

                CREATE TABLE IF NOT EXISTS genotypes_full (
                    profile_id TEXT NOT NULL,
                    rsid TEXT NOT NULL,
                    chrom TEXT NOT NULL,
                    pos INTEGER NOT NULL,
                    genotype TEXT,
                    PRIMARY KEY(profile_id, rsid)
                );

                CREATE INDEX IF NOT EXISTS idx_genotypes_full_profile_rsid
                    ON genotypes_full(profile_id, rsid);

                CREATE INDEX IF NOT EXISTS idx_genotypes_full_profile_chrom_pos
                    ON genotypes_full(profile_id, chrom, pos);

                CREATE TABLE IF NOT EXISTS insight_results (
                    id TEXT PRIMARY KEY,
                    profile_id TEXT NOT NULL,
                    module_id TEXT NOT NULL,
                    result_json TEXT NOT NULL,
                    generated_at TEXT NOT NULL,
                    kb_version TEXT NOT NULL,
                    FOREIGN KEY(profile_id) REFERENCES profiles(id)
                );
                """
            )

        if version < 2:
            self.conn.executescript(
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

                CREATE TABLE IF NOT EXISTS clinvar_imports (
                    id TEXT PRIMARY KEY,
                    file_hash_sha256 TEXT NOT NULL,
                    imported_at TEXT NOT NULL,
                    variant_count INTEGER NOT NULL
                );
                """
            )

        if version < 3:
            cur = self.conn.execute("PRAGMA table_info(imports)")
            existing = {row["name"] for row in cur.fetchall()}
            if "status" not in existing:
                self.conn.execute("ALTER TABLE imports ADD COLUMN status TEXT NOT NULL DEFAULT 'ok'")
            if "error_message" not in existing:
                self.conn.execute("ALTER TABLE imports ADD COLUMN error_message TEXT")
            if "zip_member" not in existing:
                self.conn.execute("ALTER TABLE imports ADD COLUMN zip_member TEXT")

        if version < 4:
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS clinvar_checked (
                    rsid TEXT PRIMARY KEY
                );
                """
            )

        if version < SCHEMA_VERSION:
            self.conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            self.conn.commit()

    def create_profile(self, display_name: str, notes: str | None = None) -> str:
        profile_id = safe_uuid()
        self.conn.execute(
            "INSERT INTO profiles (id, display_name, notes, created_at, encryption_enabled)"
            " VALUES (?, ?, ?, ?, ?)",
            (profile_id, display_name, notes, utc_now_iso(), 1),
        )
        self.conn.commit()
        return profile_id

    def list_profiles(self) -> list[dict]:
        cur = self.conn.execute(
            """
            SELECT p.id, p.display_name, p.notes, p.created_at, p.encryption_enabled,
                   MAX(i.imported_at) AS last_imported_at
            FROM profiles p
            LEFT JOIN imports i ON i.profile_id = p.id
            GROUP BY p.id
            ORDER BY p.created_at DESC
            """
        )
        return [dict(row) for row in cur.fetchall()]

    def get_profile(self, profile_id: str) -> dict | None:
        cur = self.conn.execute(
            "SELECT id, display_name, notes, created_at, encryption_enabled FROM profiles WHERE id = ?",
            (profile_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def rename_profile(self, profile_id: str, new_name: str) -> None:
        self.conn.execute(
            "UPDATE profiles SET display_name = ? WHERE id = ?",
            (new_name, profile_id),
        )
        self.conn.commit()

    def delete_profile(self, profile_id: str) -> None:
        self.conn.execute("DELETE FROM genotypes_curated WHERE profile_id = ?", (profile_id,))
        self.conn.execute("DELETE FROM genotypes_full WHERE profile_id = ?", (profile_id,))
        self.conn.execute("DELETE FROM insight_results WHERE profile_id = ?", (profile_id,))
        self.conn.execute("DELETE FROM imports WHERE profile_id = ?", (profile_id,))
        self.conn.execute("DELETE FROM profiles WHERE id = ?", (profile_id,))
        self.conn.commit()

    def add_import(
        self,
        profile_id: str,
        source: str,
        file_hash_sha256: str,
        parser_version: str,
        build: str,
        strand: str,
        imported_at: str | None = None,
        status: str = "ok",
        error_message: str | None = None,
        zip_member: str | None = None,
        import_id: str | None = None,
    ) -> tuple[str, str]:
        import_id = import_id or safe_uuid()
        timestamp = imported_at or utc_now_iso()
        self.conn.execute(
            """
            INSERT INTO imports (
                id, profile_id, source, file_hash_sha256, imported_at, parser_version, build, strand,
                status, error_message, zip_member
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                import_id,
                profile_id,
                source,
                file_hash_sha256,
                timestamp,
                parser_version,
                build,
                strand,
                status,
                error_message,
                zip_member,
            ),
        )
        self.conn.commit()
        return import_id, timestamp

    def update_import_status(self, import_id: str, status: str, error_message: str | None = None) -> None:
        self.conn.execute(
            "UPDATE imports SET status = ?, error_message = ? WHERE id = ?",
            (status, error_message, import_id),
        )
        self.conn.commit()

    def insert_genotypes_curated(self, rows: Iterable[tuple]) -> None:
        self.conn.executemany(
            "INSERT OR REPLACE INTO genotypes_curated (profile_id, rsid, chrom, pos, genotype)"
            " VALUES (?, ?, ?, ?, ?)",
            rows,
        )

    def insert_genotypes_full(self, rows: Iterable[tuple]) -> None:
        self.conn.executemany(
            "INSERT OR REPLACE INTO genotypes_full (profile_id, rsid, chrom, pos, genotype)"
            " VALUES (?, ?, ?, ?, ?)",
            rows,
        )

    def commit(self) -> None:
        self.conn.commit()

    def begin(self) -> None:
        self.conn.execute("BEGIN")

    def rollback(self) -> None:
        self.conn.rollback()

    def get_curated_genotypes(self, profile_id: str) -> dict[str, dict]:
        cur = self.conn.execute(
            "SELECT rsid, chrom, pos, genotype FROM genotypes_curated WHERE profile_id = ?",
            (profile_id,),
        )
        return {row["rsid"]: dict(row) for row in cur.fetchall()}

    def get_variant(self, profile_id: str, rsid: str) -> dict | None:
        cur = self.conn.execute(
            "SELECT rsid, chrom, pos, genotype FROM genotypes_curated WHERE profile_id = ? AND rsid = ?",
            (profile_id, rsid),
        )
        row = cur.fetchone()
        if row:
            return dict(row)
        cur = self.conn.execute(
            "SELECT rsid, chrom, pos, genotype FROM genotypes_full WHERE profile_id = ? AND rsid = ?",
            (profile_id, rsid),
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def store_insight_results(self, profile_id: str, results: list[dict], kb_version: str) -> str:
        generated_at = utc_now_iso()
        rows = [
            (
                safe_uuid(),
                profile_id,
                result["module_id"],
                json.dumps(result),
                generated_at,
                kb_version,
            )
            for result in results
        ]
        self.conn.executemany(
            """
            INSERT INTO insight_results (id, profile_id, module_id, result_json, generated_at, kb_version)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        self.conn.commit()
        return generated_at

    def get_latest_insights(self, profile_id: str) -> list[dict]:
        cur = self.conn.execute(
            "SELECT MAX(generated_at) AS latest FROM insight_results WHERE profile_id = ?",
            (profile_id,),
        )
        row = cur.fetchone()
        if not row or not row["latest"]:
            return []
        latest = row["latest"]
        cur = self.conn.execute(
            "SELECT result_json FROM insight_results WHERE profile_id = ? AND generated_at = ?",
            (profile_id, latest),
        )
        return [json.loads(r["result_json"]) for r in cur.fetchall()]

    def get_latest_import(self, profile_id: str) -> dict | None:
        cur = self.conn.execute(
            """
            SELECT id, profile_id, source, file_hash_sha256, imported_at, parser_version, build, strand,
                   status, error_message, zip_member
            FROM imports
            WHERE profile_id = ?
            ORDER BY imported_at DESC
            LIMIT 1
            """,
            (profile_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def upsert_clinvar_variants(self, rows: Iterable[tuple]) -> None:
        self.conn.executemany(
            """
            INSERT OR REPLACE INTO clinvar_variants
                (rsid, chrom, pos, ref, alt, clinical_significance, review_status, conditions, last_evaluated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

    def add_clinvar_import(self, file_hash_sha256: str, variant_count: int, *, commit: bool = True) -> str:
        import_id = safe_uuid()
        self.conn.execute(
            """
            INSERT INTO clinvar_imports (id, file_hash_sha256, imported_at, variant_count)
            VALUES (?, ?, ?, ?)
            """,
            (import_id, file_hash_sha256, utc_now_iso(), variant_count),
        )
        if commit:
            self.conn.commit()
        return import_id

    def get_latest_clinvar_import(self) -> dict | None:
        cur = self.conn.execute(
            """
            SELECT id, file_hash_sha256, imported_at, variant_count
            FROM clinvar_imports
            ORDER BY imported_at DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def get_clinvar_variant(self, rsid: str) -> dict | None:
        cur = self.conn.execute(
            """
            SELECT rsid, chrom, pos, ref, alt, clinical_significance, review_status, conditions, last_evaluated
            FROM clinvar_variants
            WHERE rsid = ?
            """,
            (rsid,),
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def clear_clinvar_variants(self, *, commit: bool = True) -> None:
        self.conn.execute("DELETE FROM clinvar_variants")
        if commit:
            self.conn.commit()

    def get_clinvar_checked_rsids(self) -> set[str]:
        cur = self.conn.execute("SELECT rsid FROM clinvar_checked")
        return {row["rsid"] for row in cur.fetchall()}

    def mark_clinvar_checked(self, rsids: Iterable[str], *, commit: bool = True) -> None:
        rows = [(rsid,) for rsid in rsids]
        if not rows:
            return
        self.conn.executemany("INSERT OR IGNORE INTO clinvar_checked (rsid) VALUES (?)", rows)
        if commit:
            self.conn.commit()

    def clear_clinvar_checked(self, *, commit: bool = True) -> None:
        self.conn.execute("DELETE FROM clinvar_checked")
        if commit:
            self.conn.commit()

    def get_all_rsids(self) -> set[str]:
        cur = self.conn.execute(
            """
            SELECT DISTINCT rsid FROM genotypes_full
            UNION
            SELECT DISTINCT rsid FROM genotypes_curated
            """
        )
        return {row["rsid"] for row in cur.fetchall()}

    def _has_full_genotypes(self, profile_id: str) -> bool:
        cur = self.conn.execute(
            "SELECT 1 FROM genotypes_full WHERE profile_id = ? LIMIT 1",
            (profile_id,),
        )
        return cur.fetchone() is not None

    def get_clinvar_matches(self, profile_id: str, limit: int = 5) -> list[dict]:
        table = "genotypes_full" if self._has_full_genotypes(profile_id) else "genotypes_curated"
        cur = self.conn.execute(
            f"""
            SELECT g.rsid, g.genotype, c.clinical_significance, c.review_status
            FROM {table} g
            JOIN clinvar_variants c ON g.rsid = c.rsid
            WHERE g.profile_id = ?
            LIMIT ?
            """,
            (profile_id, limit),
        )
        return [dict(row) for row in cur.fetchall()]

    def count_clinvar_matches(self, profile_id: str) -> int:
        table = "genotypes_full" if self._has_full_genotypes(profile_id) else "genotypes_curated"
        cur = self.conn.execute(
            f"""
            SELECT COUNT(*) AS total
            FROM {table} g
            JOIN clinvar_variants c ON g.rsid = c.rsid
            WHERE g.profile_id = ?
            """,
            (profile_id,),
        )
        row = cur.fetchone()
        return int(row["total"]) if row else 0
