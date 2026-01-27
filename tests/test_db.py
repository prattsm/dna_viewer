from pathlib import Path

from dna_insights.core.db import Database


def test_db_roundtrip(tmp_path: Path) -> None:
    db_path = tmp_path / "test.sqlite3"
    db = Database(db_path)

    profile_id = db.create_profile("Test")
    profiles = db.list_profiles()
    assert profiles

    db.insert_genotypes_curated([(profile_id, "rs1", "1", 100, "AA")])
    db.commit()
    variant = db.get_variant(profile_id, "rs1")
    assert variant["genotype"] == "AA"

    db.store_insight_results(profile_id, [{"module_id": "m1", "value": 1}], "0.1.0")
    insights = db.get_latest_insights(profile_id)
    assert insights[0]["module_id"] == "m1"
    db.close()


def test_import_status_and_rsids(tmp_path: Path) -> None:
    db_path = tmp_path / "status.sqlite3"
    db = Database(db_path)
    profile_id = db.create_profile("Test")

    import_id, _ = db.add_import(
        profile_id=profile_id,
        source="ancestry",
        file_hash_sha256="hash",
        parser_version="1.0",
        build="GRCh37",
        strand="+",
        status="running",
        zip_member="sample.txt",
    )
    db.update_import_status(import_id, status="ok", error_message=None)
    latest = db.get_latest_import(profile_id)
    assert latest["status"] == "ok"
    assert latest["zip_member"] == "sample.txt"

    db.insert_genotypes_curated([(profile_id, "rs1", "1", 100, "AA")])
    db.insert_genotypes_full([(profile_id, "rs2", "1", 200, "CC")])
    db.commit()
    assert db.get_all_rsids() == {"rs1", "rs2"}
    db.close()
