from __future__ import annotations

from PySide6.QtWidgets import (
    QComboBox,
    QFrame,
    QGroupBox,
    QHBoxLayout,
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
        sort_label = QLabel("Sort by")
        sort_label.setObjectName("helperLabel")
        self.sort_combo = QComboBox()
        self.sort_combo.addItem("Evidence (high → low)", "evidence_desc")
        self.sort_combo.addItem("Evidence (low → high)", "evidence_asc")
        self.sort_combo.addItem("Name (A → Z)", "name_asc")
        sort_row = QHBoxLayout()
        sort_row.addWidget(sort_label)
        sort_row.addWidget(self.sort_combo)
        sort_row.addStretch()

        card = QFrame()
        card.setObjectName("card")
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(12, 12, 12, 12)
        card_layout.addWidget(self.scroll)

        layout = QVBoxLayout()
        layout.setContentsMargins(24, 24, 24, 24)
        layout.setSpacing(16)
        layout.addWidget(title_label)
        layout.addWidget(helper_label)
        layout.addLayout(sort_row)
        layout.addWidget(card)
        self.setLayout(layout)

        self.state.profile_changed.connect(self.refresh)
        self.state.data_changed.connect(self.refresh)
        self.sort_combo.currentIndexChanged.connect(self.refresh)
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

        grouped = self._group_and_sort(results)
        for category, items in grouped:
            section = QFrame()
            section.setObjectName("card")
            section_layout = QVBoxLayout(section)
            section_layout.setContentsMargins(16, 16, 16, 16)
            section_layout.setSpacing(12)
            header = QLabel(category)
            header.setObjectName("sectionLabel")
            section_layout.addWidget(header)

            for result in items:
                evidence = result.get("evidence_level", {})
                grade = evidence.get("grade", "Unknown")
                title = f"{result.get('display_name', 'Insight')} — Evidence {grade}"
                group = QGroupBox(title)
                group_layout = QVBoxLayout()
                group_layout.addWidget(QLabel(result.get("summary", "")))
                suggestion = result.get("suggestion")
                if suggestion:
                    group_layout.addWidget(QLabel(f"Possible actions (non-medical): {suggestion}"))
                group_layout.addWidget(
                    QLabel(f"Evidence: {grade} - {evidence.get('summary', '')}")
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
                section_layout.addWidget(group)

            self.container_layout.addWidget(section)

        self.container_layout.addStretch()

    def _group_and_sort(self, results: list[dict]) -> list[tuple[str, list[dict]]]:
        category_labels = {
            "nutrition": "Nutrition",
            "wellness": "Wellness",
            "traits": "Traits",
            "pgx": "Pharmacogenomics (opt-in)",
            "clinical": "Clinical references (opt-in)",
            "qc": "Quality checks",
        }
        category_order = ["nutrition", "wellness", "traits", "pgx", "clinical", "qc"]

        sort_mode = self.sort_combo.currentData() or "evidence_desc"
        grade_order = {"A": 3, "B": 2, "C": 1}

        def evidence_score(item: dict) -> int:
            grade = (item.get("evidence_level", {}) or {}).get("grade", "")
            return grade_order.get(str(grade).upper(), 0)

        def sort_key(item: dict):
            if sort_mode == "name_asc":
                return item.get("display_name", "")
            return evidence_score(item)

        reverse = sort_mode == "evidence_desc"

        grouped: dict[str, list[dict]] = {}
        for item in results:
            grouped.setdefault(item.get("category", "other"), []).append(item)

        ordered: list[tuple[str, list[dict]]] = []
        for category in category_order:
            items = grouped.pop(category, [])
            if not items:
                continue
            items_sorted = sorted(items, key=sort_key, reverse=reverse)
            ordered.append((category_labels.get(category, category.title()), items_sorted))

        for category, items in sorted(grouped.items()):
            items_sorted = sorted(items, key=sort_key, reverse=reverse)
            ordered.append((category_labels.get(category, category.title()), items_sorted))

        return ordered
