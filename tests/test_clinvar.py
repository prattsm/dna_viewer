from pathlib import Path

from dna_insights.core.clinvar import import_clinvar_snapshot, seed_clinvar_if_missing, seed_metadata
from dna_insights.core.db import Database


def test_clinvar_import(tmp_path: Path) -> None:
    db_path = tmp_path / "clinvar.sqlite3"
    sample_path = Path("tests/fixtures/clinvar_sample.vcf")

    summary = import_clinvar_snapshot(file_path=sample_path, db_path=db_path)
    assert summary["variant_count"] == 1

    db = Database(db_path)
    assert db.get_clinvar_variant("rs123") is not None
    assert db.get_clinvar_variant("rs456") is None
    assert db.get_clinvar_variant("rs789") is None
    db.close()


def test_clinvar_seed(tmp_path: Path) -> None:
    db_path = tmp_path / "seed.sqlite3"
    db = Database(db_path)
    summary = seed_clinvar_if_missing(db)
    meta = seed_metadata()
    assert summary["seeded"] is True
    assert summary["variant_count"] == meta["variant_count"]
    assert db.get_latest_clinvar_import() is not None
    db.close()


def test_clinvar_import_empty_filter(tmp_path: Path) -> None:
    db_path = tmp_path / "empty.sqlite3"
    sample_path = Path("tests/fixtures/clinvar_sample.vcf")
    summary = import_clinvar_snapshot(file_path=sample_path, db_path=db_path, rsid_filter=set())
    assert summary["skipped"] is True


def test_variant_summary_import(tmp_path: Path) -> None:
    db_path = tmp_path / "variant_summary.sqlite3"
    sample_path = Path("tests/fixtures/variant_summary_sample.txt")
    summary = import_clinvar_snapshot(
        file_path=sample_path,
        db_path=db_path,
        rsid_filter={"rs123", "rs456", "rs789"},
    )
    assert summary["variant_count"] == 1
