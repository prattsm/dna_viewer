from __future__ import annotations

from dna_insights.core.models import EvidenceLevel, KnowledgeModule, QCReport
from dna_insights.core.utils import canonical_genotype


SENSITIVE_CATEGORIES = {"clinical", "pgx"}


def _match_rule(module: KnowledgeModule, genotypes: dict[str, str | None]) -> tuple[str | None, str]:
    for rule in module.rules:
        genotype = genotypes.get(rule.rsid)
        if genotype is None:
            continue
        canonical = canonical_genotype(genotype)
        if canonical and canonical in rule.genotypes:
            return rule.summary, rule.rsid
    return module.default_summary, None


def evaluate_modules(
    genotype_map: dict[str, dict],
    modules: list[KnowledgeModule],
    opt_in_categories: dict[str, bool],
) -> list[dict]:
    results: list[dict] = []
    for module in modules:
        if module.category in SENSITIVE_CATEGORIES and not opt_in_categories.get(module.category, False):
            continue

        module_genotypes: dict[str, str | None] = {}
        for rsid in module.rsids:
            record = genotype_map.get(rsid)
            module_genotypes[rsid] = record["genotype"] if record else None

        summary, matched_rsid = _match_rule(module, module_genotypes)
        result = {
            "module_id": module.module_id,
            "category": module.category,
            "display_name": module.display_name,
            "summary": summary,
            "suggestion": module.suggestion,
            "evidence_level": module.evidence_level.model_dump(),
            "limitations": module.limitations,
            "references": module.references,
            "genotypes": module_genotypes,
            "rule_matched": matched_rsid,
        }
        results.append(result)

    return results


def build_qc_result(qc: QCReport) -> dict:
    return {
        "module_id": "qc_summary",
        "category": "qc",
        "display_name": "Quality checks",
        "summary": (
            f"Call rate {qc.call_rate:.2%} across {qc.total_markers} markers. "
            f"Duplicates {qc.duplicates}, malformed rows {qc.malformed_rows}. "
            f"Sex check: {qc.sex_check}."
        ),
        "suggestion": None,
        "evidence_level": EvidenceLevel(grade="A", summary="Derived directly from file parsing.").model_dump(),
        "limitations": "QC is a data consistency check, not an identity or medical assessment.",
        "references": [],
        "genotypes": {},
        "rule_matched": None,
        "qc": qc.model_dump(),
    }


def build_clinvar_summary(match_count: int, sample: list[dict], import_meta: dict | None) -> dict:
    sample_text = ", ".join(item["rsid"] for item in sample) if sample else "None"
    import_note = ""
    if import_meta:
        import_note = f" ClinVar snapshot imported {import_meta.get('imported_at', '')}."
    summary = (
        f"Found {match_count} rsIDs in your data that appear in the ClinVar snapshot."
        f" Example matches: {sample_text}.{import_note}"
    )
    return {
        "module_id": "clinical_summary",
        "category": "clinical",
        "display_name": "Clinical references (ClinVar, opt-in)",
        "summary": summary,
        "suggestion": "Do not change medical care based on this app. Discuss any concerns with a clinician.",
        "evidence_level": EvidenceLevel(grade="A", summary="ClinVar listing reference only.").model_dump(),
        "limitations": (
            "SNP chip results can be wrong and do not confirm clinical significance. "
            "Only high-confidence ClinVar entries are shown, and clinical confirmation is required."
        ),
        "references": ["ClinVar (NCBI) snapshot"],
        "genotypes": {},
        "rule_matched": None,
    }
