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
