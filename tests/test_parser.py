from io import StringIO
from pathlib import Path

import pytest

from dna_insights.core.exceptions import ImportCancelled
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


def test_parse_cancelled() -> None:
    data = "".join(f"rs{i} 1 {i} A A\n" for i in range(1, 1100))
    handle = StringIO(data)

    def cancel_check() -> bool:
        return True

    with pytest.raises(ImportCancelled):
        parse_ancestry_handle(handle, on_record=lambda _record: None, cancel_check=cancel_check)
