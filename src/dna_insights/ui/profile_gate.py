from __future__ import annotations

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QFrame,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QPushButton,
    QVBoxLayout,
    QWidget,
    QInputDialog,
)

from dna_insights.app_state import AppState


class ProfileGatePage(QWidget):
    profile_selected = Signal(str)

    def __init__(self, state: AppState, parent=None) -> None:
        super().__init__(parent)
        self.state = state

        title_label = QLabel("Choose a profile")
        title_label.setObjectName("titleLabel")
        helper_label = QLabel("Select an existing profile or create a new one to continue.")
        helper_label.setObjectName("helperLabel")

        self.list_widget = QListWidget()
        self.list_widget.setSelectionMode(QListWidget.SingleSelection)
        self.list_widget.itemSelectionChanged.connect(self._update_continue_state)

        self.create_button = QPushButton("Create profile")
        self.create_button.setObjectName("secondaryButton")
        self.continue_button = QPushButton("Continue")
        self.continue_button.setObjectName("primaryButton")
        self.continue_button.setEnabled(False)

        self.create_button.clicked.connect(self._create_profile)
        self.continue_button.clicked.connect(self._continue)

        list_card = QFrame()
        list_card.setObjectName("card")
        list_layout = QVBoxLayout(list_card)
        list_layout.setContentsMargins(16, 16, 16, 16)
        list_layout.setSpacing(8)
        list_title = QLabel("Profiles")
        list_title.setObjectName("sectionLabel")
        list_layout.addWidget(list_title)
        list_layout.addWidget(self.list_widget)

        action_row = QHBoxLayout()
        action_row.setSpacing(8)
        action_row.addWidget(self.create_button)
        action_row.addStretch()
        action_row.addWidget(self.continue_button)

        layout = QVBoxLayout()
        layout.setContentsMargins(24, 24, 24, 24)
        layout.setSpacing(16)
        layout.addWidget(title_label)
        layout.addWidget(helper_label)
        layout.addWidget(list_card)
        layout.addLayout(action_row)
        layout.addStretch()
        self.setLayout(layout)

        self.state.data_changed.connect(self.refresh)
        self.refresh()

    def refresh(self) -> None:
        self.list_widget.clear()
        for profile in self.state.list_profiles():
            label = profile["display_name"]
            last_import = profile.get("last_imported_at") or "Never"
            item = QListWidgetItem(f"{label} (Last import: {last_import})")
            item.setData(Qt.UserRole, profile["id"])
            self.list_widget.addItem(item)
        self._update_continue_state()

    def _update_continue_state(self) -> None:
        self.continue_button.setEnabled(len(self.list_widget.selectedItems()) == 1)

    def _create_profile(self) -> None:
        name, ok = QInputDialog.getText(self, "Create profile", "Profile name")
        if not ok or not name.strip():
            return
        profile_id = self.state.create_profile(name.strip())
        self.state.set_current_profile(profile_id)
        self.profile_selected.emit(profile_id)

    def _continue(self) -> None:
        items = self.list_widget.selectedItems()
        if not items:
            QMessageBox.information(self, "Choose profile", "Select a profile to continue.")
            return
        profile_id = items[0].data(Qt.UserRole)
        self.state.set_current_profile(profile_id)
        self.profile_selected.emit(profile_id)
