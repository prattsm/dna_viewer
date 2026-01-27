from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path
import uuid


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def safe_uuid() -> str:
    return str(uuid.uuid4())


def sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def normalize_chrom(raw: str) -> str:
    value = raw.strip().upper()
    if value in {"23", "X"}:
        return "X"
    if value in {"24", "Y"}:
        return "Y"
    if value in {"25", "MT", "M"}:
        return "MT"
    return value


def canonical_genotype(genotype: str | None) -> str | None:
    if genotype is None:
        return None
    cleaned = genotype.replace(" ", "").upper()
    if cleaned in {"", "--", "-", "00"}:
        return None
    if len(cleaned) == 1:
        return cleaned
    if len(cleaned) == 2:
        chars = sorted(cleaned)
        return "".join(chars)
    return cleaned
