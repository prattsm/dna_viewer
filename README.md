# DNA Insights (Local)

DNA Insights is a local-first desktop app for importing **AncestryDNA raw data** and generating a clean, explainable, evidence-graded insights report. It focuses on nutrition and wellness associations, clear limitations, and privacy-first storage.

**Educational use only. Not medical advice.** Confirm any health-related findings in a clinical lab.

## Features (MVP)
- Local profiles with isolated data
- Import AncestryDNA raw data (.txt or .zip)
- Curated insights (nutrition + wellness) with evidence grades and limitations
- QC metrics (call rate, malformed rows, duplicates, X/Y consistency check)
- Variant explorer (search rsID)
- Report export (HTML/PDF) with optional encryption
- Optional ClinVar snapshot import (high-confidence filtering, opt-in clinical view)

## Safety & guardrails
- No disease risk or diagnostic claims
- No medication dosing recommendations
- Sensitive categories (clinical / pharmacogenomics) are opt-in and empty by default
- Clear limitations and evidence grading for each insight

## Setup
Prerequisites: **Python 3.11+**

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Run
```bash
dna-insights
```

If the CLI entry point is not on your PATH yet, run:
```bash
python3 -m dna_insights
```

## Tests
```bash
pytest
```

## User Guide
- **Start screen:** Choose a profile (or create one) before accessing the rest of the app.
- **Profiles:** Use the start screen or “Switch profile” in the top bar to change profiles.
- **Import:** Choose the raw data `.txt` file (or a `.zip` containing it). If the zip has multiple `.txt` files, the app asks which one to use. You can cancel an import mid-run; cancelled imports are marked as such and do not write genotypes/insights.
- **Import convenience:** The last successfully imported file path is remembered and shown next time you open the Import screen.
- **Insights:** Review evidence-graded findings with limitations and genotype context.
- **Variant Explorer:** Search by rsID to see your genotype and any matching module interpretation.
- **Report Export:** Export HTML or PDF reports; optional encryption is available.
- **Settings:** See the data directory, enable encryption, and manage opt-in categories.

## Data & privacy
- All data is stored locally in the selected data directory.
- Raw uploads are encrypted with a passphrase (Fernet via `cryptography`) and encryption is required for all profiles.
- Reports can be exported encrypted if enabled in settings.
- Environment variable `DNA_INSIGHTS_DATA_DIR` overrides the data directory for the current run.

## Knowledge base
The curated knowledge base ships with a small, conservative set of modules:
- Lactose persistence (LCT rs4988235)
- Caffeine metabolism (CYP1A2 rs762551)
- Alcohol flush (ALDH2 rs671)
- FTO trait association (rs9939609)

Each module includes evidence grading, limitations, and references that appear in reports.

## ClinVar snapshot import (optional)
- The app ships with a small bundled ClinVar snapshot so clinical references work out of the box.
- For the full ClinVar dataset, place the **variant summary** file at:
  - `<data_dir>/clinvar/variant_summary.txt.gz` (or `.txt`)
  - The app will auto-import it on launch (no manual step required), or right after your DNA import completes.
- For faster future imports, you can build a local **ClinVar cache** once and the app will use it automatically:
  - `dna-insights-build-clinvar-cache --input /path/to/variant_summary.txt`
  - Or: `python3 -m dna_insights.tools.build_clinvar_cache --input /path/to/variant_summary.txt`
  - Cache location (default): `<data_dir>/clinvar/clinvar_cache.sqlite3`
  - If the `dna-insights-build-clinvar-cache` command is not found after pulling new changes, run:
    - `pip install -e ".[dev]"`
- If you are packaging a distribution and want the full file to ship with the app, place it at:
  - `src/dna_insights/knowledge_base/clinvar_full/variant_summary.txt.gz` (or `.txt`)
  - This path is git-ignored to keep large files out of the repo.
- You can also use the VCF snapshot (`clinvar.vcf.gz`) or import via **Settings** to replace the bundled data.
- All clinical significance categories are stored, but the UI surfaces review status, confidence level, and conflicts clearly.
- The app still stores only variants that match rsIDs found in the user’s imported DNA file.
- Any clinical references are informational only and require clinical confirmation.

## License
Free to use under the MIT License.
