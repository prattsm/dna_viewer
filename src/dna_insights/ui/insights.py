from __future__ import annotations

from PySide6.QtWidgets import (
    QGroupBox,
    QLabel,
    QScrollArea,
    QVBoxLayout,
    QWidget,
)

from dna_insights.app_state import AppState
from dna_insights.core.insight_engine import build_clinvar_summary


class InsightsPage(QWidget):
    def __init__(self, state: AppState, parent=None) -> None:
        super().__init__(parent)
        self.state = state

        self.scroll = QScrollArea()
        self.scroll.setWidgetResizable(True)
        self.container = QWidget()
        self.container_layout = QVBoxLayout()
        self.container.setLayout(self.container_layout)
        self.scroll.setWidget(self.container)

        title_label = QLabel("Insights")
        title_label.setObjectName("titleLabel")
        helper_label = QLabel("Evidence-graded summaries based on your imported DNA.")
        helper_label.setObjectName("helperLabel")

        layout = QVBoxLayout()
        layout.setContentsMargins(24, 24, 24, 24)
        layout.setSpacing(16)
        layout.addWidget(title_label)
        layout.addWidget(helper_label)
        layout.addWidget(self.scroll)
        self.setLayout(layout)

        self.state.profile_changed.connect(self.refresh)
        self.state.data_changed.connect(self.refresh)
        self.refresh()

    def _clear(self) -> None:
        while self.container_layout.count():
            item = self.container_layout.takeAt(0)
            widget = item.widget()
            if widget:
                widget.deleteLater()

    def refresh(self) -> None:
        self._clear()
        profile = self.state.current_profile()
        if not profile:
            self.container_layout.addWidget(QLabel("Select a profile to view insights."))
            return
        results = self.state.db.get_latest_insights(profile["id"])
        if not results:
            self.container_layout.addWidget(QLabel("No insights yet. Import a file first."))
            return

        if self.state.settings.opt_in_categories.get("clinical", False):
            clinvar_import = self.state.db.get_latest_clinvar_import()
            if clinvar_import:
                count = self.state.db.count_clinvar_matches(profile["id"])
                sample = self.state.db.get_clinvar_matches(profile["id"], limit=3)
                results.append(build_clinvar_summary(count, sample, clinvar_import))

        for result in results:
            group = QGroupBox(result.get("display_name", "Insight"))
            group_layout = QVBoxLayout()
            group_layout.addWidget(QLabel(result.get("summary", "")))
            suggestion = result.get("suggestion")
            if suggestion:
                group_layout.addWidget(QLabel(f"Possible actions (non-medical): {suggestion}"))
            evidence = result.get("evidence_level", {})
            group_layout.addWidget(
                QLabel(f"Evidence: {evidence.get('grade', '')} - {evidence.get('summary', '')}")
            )
            group_layout.addWidget(QLabel(f"Limitations: {result.get('limitations', '')}"))

            genotypes = result.get("genotypes", {})
            if genotypes:
                lines = ", ".join(f"{rsid}: {geno}" for rsid, geno in genotypes.items())
                group_layout.addWidget(QLabel(f"Genotypes: {lines}"))

            references = result.get("references", [])
            if references:
                group_layout.addWidget(QLabel("References: " + "; ".join(references)))

            group.setLayout(group_layout)
            self.container_layout.addWidget(group)

        self.container_layout.addStretch()
