from __future__ import annotations

from PySide6.QtWidgets import (
    QLabel,
    QListWidget,
    QMainWindow,
    QHBoxLayout,
    QStackedWidget,
    QVBoxLayout,
    QWidget,
)

from dna_insights.app_state import AppState
from dna_insights.constants import APP_NAME
from dna_insights.ui.import_wizard import ImportPage
from dna_insights.ui.insights import InsightsPage
from dna_insights.ui.profiles import ProfilesPage
from dna_insights.ui.report_export import ReportExportPage
from dna_insights.ui.settings import SettingsPage
from dna_insights.ui.variant_explorer import VariantExplorerPage


class MainWindow(QMainWindow):
    def __init__(self, state: AppState, parent=None) -> None:
        super().__init__(parent)
        self.state = state
        self.setWindowTitle(APP_NAME)
        self.resize(1000, 700)

        banner = QLabel("Educational only. Not medical advice. Confirm health-related findings clinically.")
        banner.setStyleSheet("padding: 8px; background: #ffe9d6; border-radius: 6px;")

        self.nav = QListWidget()
        self.nav.addItems([
            "Profiles",
            "Import",
            "Insights",
            "Variant Explorer",
            "Report Export",
            "Settings",
        ])

        self.stack = QStackedWidget()
        self.pages = [
            ProfilesPage(state),
            ImportPage(state),
            InsightsPage(state),
            VariantExplorerPage(state),
            ReportExportPage(state),
            SettingsPage(state),
        ]
        for page in self.pages:
            self.stack.addWidget(page)

        self.nav.currentRowChanged.connect(self.stack.setCurrentIndex)
        self.nav.setCurrentRow(0)

        content_layout = QHBoxLayout()
        content_layout.addWidget(self.nav)
        content_layout.addWidget(self.stack, 1)

        main_layout = QVBoxLayout()
        main_layout.addWidget(banner)
        main_layout.addLayout(content_layout)

        container = QWidget()
        container.setLayout(main_layout)
        self.setCentralWidget(container)
