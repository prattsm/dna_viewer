from __future__ import annotations

from PySide6.QtCore import QUrl
from PySide6.QtGui import QDesktopServices
from PySide6.QtWidgets import (
    QCheckBox,
    QLabel,
    QMessageBox,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from dna_insights.app_state import AppState
from dna_insights.core.settings import resolve_data_dir, save_settings
from dna_insights.ui.widgets import prompt_passphrase


class SettingsPage(QWidget):
    def __init__(self, state: AppState, parent=None) -> None:
        super().__init__(parent)
        self.state = state

        self.data_dir_label = QLabel("")
        self.open_data_button = QPushButton("Open data folder")

        self.encryption_checkbox = QCheckBox("Encrypt stored raw imports and reports")
        self.encryption_checkbox.setChecked(self.state.settings.encryption_enabled)

        self.clinical_checkbox = QCheckBox("Enable clinical-style insights (not included by default)")
        self.clinical_checkbox.setChecked(self.state.settings.opt_in_categories.get("clinical", False))

        self.pgx_checkbox = QCheckBox("Enable pharmacogenomics insights (not included by default)")
        self.pgx_checkbox.setChecked(self.state.settings.opt_in_categories.get("pgx", False))

        self.kb_label = QLabel(f"Knowledge base version: {self.state.manifest.kb_version}")
        self.banner = QLabel("Educational only. Not medical advice. Confirm health-related findings clinically.")

        layout = QVBoxLayout()
        layout.addWidget(QLabel("Data directory"))
        layout.addWidget(self.data_dir_label)
        layout.addWidget(self.open_data_button)
        layout.addWidget(self.encryption_checkbox)
        layout.addWidget(self.clinical_checkbox)
        layout.addWidget(self.pgx_checkbox)
        layout.addWidget(self.kb_label)
        layout.addWidget(self.banner)
        layout.addStretch()
        self.setLayout(layout)

        self.open_data_button.clicked.connect(self._open_data_dir)
        self.encryption_checkbox.toggled.connect(self._toggle_encryption)
        self.clinical_checkbox.toggled.connect(self._toggle_opt_in)
        self.pgx_checkbox.toggled.connect(self._toggle_opt_in)

        self.refresh()

    def refresh(self) -> None:
        data_dir = resolve_data_dir(self.state.settings)
        self.data_dir_label.setText(str(data_dir))

    def _open_data_dir(self) -> None:
        data_dir = resolve_data_dir(self.state.settings)
        QDesktopServices.openUrl(QUrl.fromLocalFile(str(data_dir)))

    def _toggle_encryption(self, checked: bool) -> None:
        if checked and not self.state.settings.encryption_enabled:
            passphrase = prompt_passphrase(self, confirm=True)
            if not passphrase:
                QMessageBox.information(self, "Encryption", "Passphrase not set. Encryption remains disabled.")
                self.encryption_checkbox.setChecked(False)
                return
            self.state.settings.encryption_enabled = True
            self.state.encryption.unlock(passphrase)
            save_settings(self.state.settings)
            self.state.data_changed.emit()
            QMessageBox.information(self, "Encryption", "Encryption enabled for stored files.")
            return

        if not checked and self.state.settings.encryption_enabled:
            confirm = QMessageBox.question(
                self,
                "Disable encryption",
                "Disable encryption for future files? Existing encrypted files remain encrypted.",
            )
            if confirm == QMessageBox.StandardButton.Yes:
                self.state.settings.encryption_enabled = False
                self.state.encryption.lock()
                save_settings(self.state.settings)
                self.state.data_changed.emit()
            else:
                self.encryption_checkbox.setChecked(True)

    def _toggle_opt_in(self) -> None:
        self.state.settings.opt_in_categories["clinical"] = self.clinical_checkbox.isChecked()
        self.state.settings.opt_in_categories["pgx"] = self.pgx_checkbox.isChecked()
        save_settings(self.state.settings)
        self.state.data_changed.emit()
