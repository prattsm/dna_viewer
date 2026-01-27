from __future__ import annotations

import json
from importlib import resources
from typing import Iterable

from dna_insights.core.models import KnowledgeBaseManifest, KnowledgeModule


def _kb_root():
    return resources.files("dna_insights.knowledge_base")


def load_manifest() -> KnowledgeBaseManifest:
    manifest_path = _kb_root() / "kb_manifest.json"
    data = json.loads(manifest_path.read_text())
    return KnowledgeBaseManifest(**data)


def load_modules(manifest: KnowledgeBaseManifest) -> list[KnowledgeModule]:
    modules: list[KnowledgeModule] = []
    for module_file in manifest.module_files:
        module_path = _kb_root() / "modules" / module_file
        data = json.loads(module_path.read_text())
        modules.append(KnowledgeModule(**data))
    return modules


def curated_rsids(modules: Iterable[KnowledgeModule]) -> set[str]:
    rsids: set[str] = set()
    for module in modules:
        rsids.update(module.rsids)
    return rsids
