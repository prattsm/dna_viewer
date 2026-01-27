from pathlib import Path

from dna_insights.core.parser import parse_ancestry_handle


def test_parse_ancestry_file(tmp_path: Path) -> None:
    sample_path = Path("tests/fixtures/ancestry_sample.txt")
    collected = []

    def on_record(record):
        collected.append(record)

    with sample_path.open("r", encoding="utf-8") as handle:
        stats = parse_ancestry_handle(handle, on_record=on_record)

    assert stats.total_markers == 6
    assert stats.missing_calls == 1
    assert stats.duplicates == 1
    assert stats.malformed_rows == 1
    assert "Insufficient" in stats.sex_check()

    rsids = [item.rsid for item in collected]
    assert "rs4988235" in rsids
    assert collected[0].genotype == "CT"
