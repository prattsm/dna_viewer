from pathlib import Path

from dna_insights.core.clinvar import import_clinvar_snapshot
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
