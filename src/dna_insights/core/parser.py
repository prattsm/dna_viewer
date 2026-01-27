from __future__ import annotations

import io
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterable
import zipfile

from dna_insights.core.utils import canonical_genotype, normalize_chrom

PARSER_VERSION = "1.0"


@dataclass
class ParseStats:
    total_markers: int = 0
    missing_calls: int = 0
    duplicates: int = 0
    malformed_rows: int = 0
    warnings: list[str] = field(default_factory=list)
    x_calls: int = 0
    y_calls: int = 0

    def call_rate(self) -> float:
        if self.total_markers == 0:
            return 0.0
        return (self.total_markers - self.missing_calls) / self.total_markers

    def sex_check(self) -> str:
        if self.y_calls > 0:
            return "Y markers present (XY pattern likely)"
        if self.x_calls > 0 and self.y_calls == 0:
            return "No Y markers detected (XX pattern likely)"
        return "Insufficient X/Y data for a consistency check"


@dataclass
class ParsedRecord:
    rsid: str
    chrom: str
    pos: int
    genotype: str | None


def list_zip_txt_members(path: Path) -> list[str]:
    with zipfile.ZipFile(path) as zip_file:
        return [name for name in zip_file.namelist() if name.lower().endswith(".txt")]


def _open_text_from_zip(path: Path, member: str | None) -> io.TextIOBase:
    zip_file = zipfile.ZipFile(path)
    txt_members = [name for name in zip_file.namelist() if name.lower().endswith(".txt")]
    if not txt_members:
        zip_file.close()
        raise ValueError("Zip file does not contain a .txt raw data export.")
    if member is None:
        if len(txt_members) == 1:
            member = txt_members[0]
        else:
            zip_file.close()
            raise ValueError("Zip file contains multiple .txt files; please choose one.")
    raw_handle = zip_file.open(member, "r")
    text_handle = io.TextIOWrapper(raw_handle, encoding="utf-8", errors="replace")
    text_handle._zip_file = zip_file  # type: ignore[attr-defined]
    return text_handle


def _close_zip_handle(handle: io.TextIOBase) -> None:
    zip_file = getattr(handle, "_zip_file", None)
    try:
        handle.close()
    finally:
        if zip_file is not None:
            zip_file.close()


def close_ancestry_handle(handle: io.TextIOBase) -> None:
    _close_zip_handle(handle)


def open_ancestry_file(path: Path, member: str | None = None) -> io.TextIOBase:
    if path.suffix.lower() == ".zip":
        return _open_text_from_zip(path, member)
    return path.open("r", encoding="utf-8", errors="replace")


def ancestry_text_total_bytes(path: Path, member: str | None = None) -> int:
    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as zip_file:
            txt_members = [name for name in zip_file.namelist() if name.lower().endswith(".txt")]
            if not txt_members:
                return 0
            if member is None:
                if len(txt_members) == 1:
                    member = txt_members[0]
                else:
                    return 0
            info = zip_file.getinfo(member)
            return int(info.file_size)
    try:
        return int(path.stat().st_size)
    except FileNotFoundError:
        return 0


def parse_ancestry_handle(
    handle: io.TextIOBase,
    on_record: Callable[[ParsedRecord], None],
    on_progress: Callable[[int], None] | None = None,
    on_bytes: Callable[[int], None] | None = None,
) -> ParseStats:
    stats = ParseStats()
    seen_rsids: set[str] = set()
    header_checked = False
    header_has_ancestry = False

    bytes_read = 0
    for line_number, line in enumerate(handle, start=1):
        if on_bytes:
            bytes_read += len(line.encode("utf-8", errors="ignore"))
            if bytes_read % (1024 * 256) < len(line):
                on_bytes(bytes_read)
        if line.startswith("#"):
            if not header_checked and "ancestry" in line.lower():
                header_has_ancestry = True
            if line_number > 20:
                header_checked = True
            continue

        if not header_checked and line_number > 20:
            header_checked = True

        parts = line.strip().split()
        if len(parts) < 5:
            stats.malformed_rows += 1
            continue

        rsid, chrom_raw, pos_raw, allele1, allele2 = parts[:5]
        try:
            pos = int(pos_raw)
        except ValueError:
            stats.malformed_rows += 1
            continue

        chrom = normalize_chrom(chrom_raw)
        allele1 = allele1.strip().upper()
        allele2 = allele2.strip().upper()
        genotype = None
        if allele1 not in {"0", "-", "--"} and allele2 not in {"0", "-", "--"}:
            genotype = canonical_genotype(allele1 + allele2)

        if rsid in seen_rsids:
            stats.duplicates += 1
        else:
            seen_rsids.add(rsid)

        stats.total_markers += 1
        if genotype is None:
            stats.missing_calls += 1
        else:
            if chrom == "X":
                stats.x_calls += 1
            elif chrom == "Y":
                stats.y_calls += 1

        on_record(ParsedRecord(rsid=rsid, chrom=chrom, pos=pos, genotype=genotype))

        if on_progress and stats.total_markers % 10000 == 0:
            on_progress(stats.total_markers)

    if on_progress:
        on_progress(stats.total_markers)
    if on_bytes:
        on_bytes(bytes_read)
    if not header_has_ancestry:
        stats.warnings.append("Header does not mention AncestryDNA; verify file source.")
    return stats
