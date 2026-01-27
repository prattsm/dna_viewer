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

## Tests
```bash
pytest
```

## User Guide
- **Profiles:** Create a profile for each person. All data stays local.
- **Import:** Choose a raw data file and import in curated (default) or full mode. Import runs in the background.
- **Insights:** Review evidence-graded findings with limitations and genotype context.
- **Variant Explorer:** Search by rsID to see your genotype and any matching module interpretation.
- **Report Export:** Export HTML or PDF reports; optional encryption is available.
- **Settings:** See the data directory, enable encryption, and manage opt-in categories.

## Data & privacy
- All data is stored locally in the selected data directory.
- Raw uploads can be stored encrypted with a passphrase (Fernet via `cryptography`).
- Reports can be exported encrypted if enabled in settings.
- Environment variable `DNA_INSIGHTS_DATA_DIR` overrides the data directory for the current run.

## Knowledge base
The curated knowledge base ships with a small, conservative set of modules:
- Lactose persistence (LCT rs4988235)
- Caffeine metabolism (CYP1A2 rs762551)
- Alcohol flush (ALDH2 rs671)
- FTO trait association (rs9939609)

Each module includes evidence grading, limitations, and references that appear in reports.
