from pathlib import Path

from dna_insights.core.clinvar import (
    build_clinvar_cache,
    classify_clinvar,
    import_clinvar_cache,
    import_clinvar_snapshot,
    seed_clinvar_if_missing,
    seed_metadata,
)
from dna_insights.core.db import Database


def test_clinvar_import(tmp_path: Path) -> None:
    db_path = tmp_path / "clinvar.sqlite3"
    sample_path = Path("tests/fixtures/clinvar_sample.vcf")

    summary = import_clinvar_snapshot(file_path=sample_path, db_path=db_path)
    assert summary["variant_count"] == 3

    db = Database(db_path)
    assert db.get_clinvar_variant("rs123") is not None
    assert db.get_clinvar_variant("rs456") is not None
    assert db.get_clinvar_variant("rs789") is not None
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
    assert summary["variant_count"] == 3


def test_clinvar_cache_import(tmp_path: Path) -> None:
    cache_path = tmp_path / "clinvar_cache.sqlite3"
    sample_path = Path("tests/fixtures/variant_summary_sample.txt")
    build_summary = build_clinvar_cache(input_path=sample_path, output_path=cache_path)
    assert build_summary["variant_count"] >= 3

    db_path = tmp_path / "cache_import.sqlite3"
    summary = import_clinvar_cache(
        cache_path=cache_path,
        db_path=db_path,
        rsid_filter={"rs123", "rs456", "rs789"},
    )
    assert summary["variant_count"] == 3


def test_clinvar_cache_import_new_rsids(tmp_path: Path) -> None:
    cache_path = tmp_path / "clinvar_cache.sqlite3"
    sample_path = Path("tests/fixtures/variant_summary_sample.txt")
    build_clinvar_cache(input_path=sample_path, output_path=cache_path)

    db_path = tmp_path / "cache_increment.sqlite3"
    summary1 = import_clinvar_cache(
        cache_path=cache_path,
        db_path=db_path,
        rsid_filter={"rs123"},
    )
    assert summary1["variant_count"] == 1

    summary2 = import_clinvar_cache(
        cache_path=cache_path,
        db_path=db_path,
        rsid_filter={"rs456"},
    )
    assert summary2.get("skipped") is not True
    assert summary2["variant_count"] == 1


def test_clinvar_classification() -> None:
    flags = classify_clinvar("Conflicting interpretations of pathogenicity", "criteria provided, single submitter")
    assert flags["conflict"] is True
    assert flags["confidence"] == "Low"
