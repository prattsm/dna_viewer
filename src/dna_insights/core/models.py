from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ModuleRule(BaseModel):
    rsid: str
    genotypes: list[str]
    summary: str


class EvidenceLevel(BaseModel):
    grade: str
    summary: str


class KnowledgeModule(BaseModel):
    module_id: str
    category: str
    display_name: str
    rsids: list[str]
    rules: list[ModuleRule]
    default_summary: str
    suggestion: str | None = None
    evidence_level: EvidenceLevel
    limitations: str
    references: list[str]


class KnowledgeBaseManifest(BaseModel):
    kb_version: str
    build: str
    strand: str
    module_files: list[str] = Field(default_factory=list)


class InsightResult(BaseModel):
    module_id: str
    category: str
    display_name: str
    summary: str
    suggestion: str | None = None
    evidence_level: EvidenceLevel
    limitations: str
    references: list[str]
    genotypes: dict[str, str | None]
    rule_matched: str | None = None


class QCReport(BaseModel):
    total_markers: int
    missing_calls: int
    call_rate: float
    duplicates: int
    malformed_rows: int
    sex_check: str
    warnings: list[str] = Field(default_factory=list)


class ImportSummary(BaseModel):
    import_id: str
    profile_id: str
    source: str
    file_hash_sha256: str
    imported_at: str
    parser_version: str
    build: str
    strand: str
    qc_report: QCReport
    insight_count: int
    kb_version: str
    curated_mode: bool
    full_mode: bool
