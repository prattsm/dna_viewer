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
from dna_insights.ui.profile_gate import ProfileGatePage
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
        banner.setObjectName("bannerLabel")

        self.nav = QListWidget()
        self.nav.setObjectName("navList")
        self.nav.addItems([
            "Profiles",
            "Import",
            "Insights",
            "Variant Explorer",
            "Report Export",
            "Settings",
        ])

        self.stack = QStackedWidget()
        self.profiles_page = ProfilesPage(state)
        self.import_page = ImportPage(state)
        self.import_page.manage_profiles_requested.connect(lambda: self.nav.setCurrentRow(0))
        self.pages = [
            self.profiles_page,
            self.import_page,
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
        content_layout.setSpacing(16)
        content_layout.addWidget(self.nav)
        content_layout.addWidget(self.stack, 1)
        self.main_container = QWidget()
        self.main_container.setLayout(content_layout)

        self.gate_page = ProfileGatePage(state)
        self.gate_page.profile_selected.connect(self._show_main_content)
        self.outer_stack = QStackedWidget()
        self.outer_stack.addWidget(self.gate_page)
        self.outer_stack.addWidget(self.main_container)

        main_layout = QVBoxLayout()
        main_layout.setContentsMargins(16, 16, 16, 16)
        main_layout.setSpacing(16)
        main_layout.addWidget(banner)
        main_layout.addWidget(self.outer_stack)

        container = QWidget()
        container.setObjectName("appRoot")
        container.setLayout(main_layout)
        self.setCentralWidget(container)

        self.state.profile_changed.connect(self._sync_profile_gate)
        self._sync_profile_gate(self.state.current_profile_id or "")

    def _show_main_content(self, _profile_id: str) -> None:
        self._sync_profile_gate(self.state.current_profile_id or "")

    def _sync_profile_gate(self, profile_id: str) -> None:
        if profile_id:
            if self.outer_stack.currentWidget() is self.gate_page:
                self.nav.setCurrentRow(1)
            self.outer_stack.setCurrentWidget(self.main_container)
        else:
            self.outer_stack.setCurrentWidget(self.gate_page)
