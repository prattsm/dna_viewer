from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
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


class ProfilesPage(QWidget):
    def __init__(self, state: AppState, parent=None) -> None:
        super().__init__(parent)
        self.state = state

        self.list_widget = QListWidget()
        self.list_widget.itemSelectionChanged.connect(self._on_selection_changed)

        self.new_button = QPushButton("Create profile")
        self.rename_button = QPushButton("Rename")
        self.delete_button = QPushButton("Delete")

        self.new_button.clicked.connect(self._create_profile)
        self.rename_button.clicked.connect(self._rename_profile)
        self.delete_button.clicked.connect(self._delete_profile)

        title_label = QLabel("Profiles")
        title_label.setObjectName("titleLabel")
        helper_label = QLabel("Create separate local profiles for each person.")
        helper_label.setObjectName("helperLabel")

        button_row = QHBoxLayout()
        button_row.addWidget(self.new_button)
        button_row.addWidget(self.rename_button)
        button_row.addWidget(self.delete_button)
        button_row.addStretch()

        layout = QVBoxLayout()
        layout.setContentsMargins(24, 24, 24, 24)
        layout.setSpacing(16)
        layout.addWidget(title_label)
        layout.addWidget(helper_label)
        layout.addWidget(self.list_widget)
        layout.addLayout(button_row)
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

    def _on_selection_changed(self) -> None:
        items = self.list_widget.selectedItems()
        if not items:
            self.state.set_current_profile(None)
            return
        profile_id = items[0].data(Qt.UserRole)
        self.state.set_current_profile(profile_id)

    def _create_profile(self) -> None:
        name, ok = QInputDialog.getText(self, "Create profile", "Profile name")
        if not ok or not name.strip():
            return
        profile_id = self.state.create_profile(name.strip())
        self.state.set_current_profile(profile_id)

    def _rename_profile(self) -> None:
        profile = self.state.current_profile()
        if not profile:
            QMessageBox.information(self, "Rename profile", "Select a profile first.")
            return
        name, ok = QInputDialog.getText(self, "Rename profile", "New name", text=profile["display_name"])
        if not ok or not name.strip():
            return
        self.state.rename_profile(profile["id"], name.strip())

    def _delete_profile(self) -> None:
        profile = self.state.current_profile()
        if not profile:
            QMessageBox.information(self, "Delete profile", "Select a profile first.")
            return
        confirm = QMessageBox.question(
            self,
            "Delete profile",
            f"Delete profile '{profile['display_name']}' and all local data?",
        )
        if confirm == QMessageBox.StandardButton.Yes:
            self.state.delete_profile(profile["id"])
