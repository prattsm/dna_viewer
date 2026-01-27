from dna_insights.core.insight_engine import evaluate_modules
from dna_insights.core.knowledge_base import load_manifest, load_modules


def test_evaluate_modules() -> None:
    manifest = load_manifest()
    modules = load_modules(manifest)
    genotype_map = {
        "rs4988235": {"genotype": "CT"},
        "rs762551": {"genotype": "AA"},
        "rs671": {"genotype": "GG"},
        "rs9939609": {"genotype": "TT"},
    }
    results = evaluate_modules(genotype_map, modules, {"clinical": False, "pgx": False})
    summaries = {result["module_id"]: result["summary"] for result in results}
    assert "lactose_persistence" in summaries
    assert "T allele" in summaries["lactose_persistence"]
    assert "caffeine" in summaries["caffeine_metabolism"].lower()
