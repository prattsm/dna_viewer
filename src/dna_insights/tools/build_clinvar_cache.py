from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from dna_insights.core.clinvar import CLINVAR_CACHE_FILENAME, build_clinvar_cache
from dna_insights.core.settings import load_settings, resolve_data_dir


def _default_output_path() -> Path:
    settings, _ = load_settings()
    data_dir = resolve_data_dir(settings)
    return data_dir / "clinvar" / CLINVAR_CACHE_FILENAME


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build a local ClinVar cache database.")
    parser.add_argument("--input", required=True, help="Path to ClinVar variant_summary.txt(.gz) or VCF")
    parser.add_argument("--output", help="Output sqlite path (default: <data_dir>/clinvar/clinvar_cache.sqlite3)")
    args = parser.parse_args(argv)

    input_path = Path(args.input).expanduser().resolve()
    if not input_path.exists():
        print(f"Input file not found: {input_path}")
        return 2

    output_path = Path(args.output).expanduser().resolve() if args.output else _default_output_path()

    last_emit = 0
    start_time = time.monotonic()

    def on_progress_detail(percent: int, _bytes_read: int, eta_seconds: float) -> None:
        nonlocal last_emit
        if percent == last_emit:
            return
        last_emit = percent
        elapsed = max(time.monotonic() - start_time, 0.001)
        rate = percent / elapsed if elapsed > 0 else 0.0
        eta_display = ""
        if eta_seconds > 0:
            minutes, seconds = divmod(int(eta_seconds), 60)
            hours, minutes = divmod(minutes, 60)
            eta_display = f" ETA {hours:02d}:{minutes:02d}:{seconds:02d}" if hours else f" ETA {minutes:02d}:{seconds:02d}"
        print(f"[{percent:3d}%] building cache...{eta_display}")

    summary = build_clinvar_cache(
        input_path=input_path,
        output_path=output_path,
        on_progress_detail=on_progress_detail,
    )

    print(
        f"Cache built at {output_path}\n"
        f"Variants stored: {summary.get('variant_count')}\n"
        f"Source hash: {summary.get('file_hash_sha256')}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
