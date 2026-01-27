from __future__ import annotations

from PySide6.QtWidgets import (
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from dna_insights.app_state import AppState
from dna_insights.core.clinvar import classify_clinvar
from dna_insights.core.insight_engine import evaluate_modules


class VariantExplorerPage(QWidget):
    def __init__(self, state: AppState, parent=None) -> None:
        super().__init__(parent)
        self.state = state

        self.input = QLineEdit()
        self.search_button = QPushButton("Search rsID")
        self.result_label = QLabel("")

        layout = QVBoxLayout()
        layout.addWidget(QLabel("rsID"))
        layout.addWidget(self.input)
        layout.addWidget(self.search_button)
        layout.addWidget(self.result_label)
        layout.addStretch()
        self.setLayout(layout)

        self.search_button.clicked.connect(self._search)

    def _search(self) -> None:
        profile = self.state.current_profile()
        if not profile:
            QMessageBox.information(self, "Variant explorer", "Select a profile first.")
            return
        rsid = self.input.text().strip()
        if not rsid:
            return
        record = self.state.db.get_variant(profile["id"], rsid)
        if not record:
            self.result_label.setText("Variant not found in this profile.")
            return

        genotype = record.get("genotype")
        base_text = f"{rsid}: {genotype} (chr {record.get('chrom')}:{record.get('pos')})"

        matched_modules = [module for module in self.state.modules if rsid in module.rsids]
        if not matched_modules:
            clinvar_info = None
            if self.state.settings.opt_in_categories.get("clinical", False):
                clinvar_info = self.state.db.get_clinvar_variant(rsid)
            if clinvar_info:
                flags = classify_clinvar(
                    clinvar_info.get("clinical_significance", ""),
                    clinvar_info.get("review_status", ""),
                )
                conflict_text = "Yes" if flags["conflict"] else "No"
                extra = (
                    f"\nClinVar: {clinvar_info.get('clinical_significance', '')}"
                    f" (review: {clinvar_info.get('review_status', '')})"
                    f"\nConfidence: {flags['confidence']}; Conflicting interpretations: {conflict_text}"
                )
                self.result_label.setText(base_text + extra)
            else:
                self.result_label.setText(base_text)
            return

        genotype_map = {rsid: record}
        results = evaluate_modules(genotype_map, matched_modules, self.state.settings.opt_in_categories)
        summaries = "\n".join(f"{item['display_name']}: {item['summary']}" for item in results)
        clinvar_info = None
        if self.state.settings.opt_in_categories.get("clinical", False):
            clinvar_info = self.state.db.get_clinvar_variant(rsid)
        if clinvar_info:
            flags = classify_clinvar(
                clinvar_info.get("clinical_significance", ""),
                clinvar_info.get("review_status", ""),
            )
            conflict_text = "Yes" if flags["conflict"] else "No"
            summaries += (
                f"\nClinVar: {clinvar_info.get('clinical_significance', '')}"
                f" (review: {clinvar_info.get('review_status', '')})"
                f"\nConfidence: {flags['confidence']}; Conflicting interpretations: {conflict_text}"
            )
        self.result_label.setText(base_text + "\n" + summaries)
