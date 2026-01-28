from __future__ import annotations

from PySide6.QtWidgets import (
    QLabel,
    QListWidget,
    QMainWindow,
    QHBoxLayout,
    QPushButton,
    QStackedWidget,
    QVBoxLayout,
    QWidget,
    QFrame,
)

from dna_insights.app_state import AppState
from dna_insights.constants import APP_NAME
from dna_insights.ui.import_wizard import ImportPage
from dna_insights.ui.insights import InsightsPage
from dna_insights.ui.profile_gate import ProfileGatePage
from dna_insights.ui.report_export import ReportExportPage
from dna_insights.ui.settings import SettingsPage
from dna_insights.ui.variant_explorer import VariantExplorerPage


class MainWindow(QMainWindow):
    def __init__(self, state: AppState, parent=None) -> None:
        super().__init__(parent)
        self.state = state
        self.setWindowTitle(APP_NAME)
        self.resize(1000, 700)

        self.top_bar = QFrame()
        self.top_bar.setObjectName("topBar")
        top_layout = QHBoxLayout(self.top_bar)
        top_layout.setContentsMargins(16, 12, 16, 12)
        top_layout.setSpacing(12)
        app_label = QLabel(APP_NAME)
        app_label.setObjectName("titleLabel")
        self.profile_chip = QLabel("No profile selected")
        self.profile_chip.setObjectName("profileChip")
        self.switch_profile_button = QPushButton("Switch profile")
        self.switch_profile_button.setObjectName("secondaryButton")
        self.switch_profile_button.clicked.connect(self._show_profile_gate)
        top_layout.addWidget(app_label)
        top_layout.addStretch()
        top_layout.addWidget(self.profile_chip)
        top_layout.addWidget(self.switch_profile_button)

        banner = QLabel("Educational only. Not medical advice. Confirm health-related findings clinically.")
        banner.setObjectName("bannerLabel")

        self.nav = QListWidget()
        self.nav.setObjectName("navList")
        self.nav.addItems([
            "Import",
            "Insights",
            "Variant Explorer",
            "Report Export",
            "Settings",
        ])

        self.stack = QStackedWidget()
        self.import_page = ImportPage(state)
        self.import_page.switch_profile_requested.connect(self._show_profile_gate)
        self.pages = [
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
        main_layout.addWidget(self.top_bar)
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

    def _show_profile_gate(self) -> None:
        self.state.set_current_profile(None)

    def _sync_profile_gate(self, profile_id: str) -> None:
        if profile_id:
            if self.outer_stack.currentWidget() is self.gate_page:
                self.nav.setCurrentRow(0)
            self.outer_stack.setCurrentWidget(self.main_container)
        else:
            self.outer_stack.setCurrentWidget(self.gate_page)
        self._update_profile_badge(profile_id)

    def _update_profile_badge(self, profile_id: str) -> None:
        profile = self.state.current_profile()
        if profile_id and profile:
            self.profile_chip.setText(profile.get("display_name", "Profile"))
            self.profile_chip.setVisible(True)
            self.switch_profile_button.setVisible(True)
        else:
            self.profile_chip.setText("No profile selected")
            self.profile_chip.setVisible(False)
            self.switch_profile_button.setVisible(False)
