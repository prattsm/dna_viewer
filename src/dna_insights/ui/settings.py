from __future__ import annotations

from pathlib import Path

from PySide6.QtCore import QObject, QThread, QUrl, Signal
from PySide6.QtGui import QDesktopServices
from PySide6.QtWidgets import (
    QCheckBox,
    QFileDialog,
    QLabel,
    QMessageBox,
    QPushButton,
    QProgressDialog,
    QVBoxLayout,
    QWidget,
)

from dna_insights.app_state import AppState
from dna_insights.core.clinvar import import_clinvar_snapshot, seed_metadata
from dna_insights.core.settings import resolve_data_dir, save_settings


class ClinVarImportWorker(QObject):
    progress = Signal(int)
    finished = Signal(dict)
    error = Signal(str)

    def __init__(self, db_path: Path, file_path: Path) -> None:
        super().__init__()
        self.db_path = db_path
        self.file_path = file_path

    def run(self) -> None:
        try:
            summary = import_clinvar_snapshot(
                file_path=self.file_path,
                db_path=self.db_path,
                on_progress=self.progress.emit,
                replace=True,
            )
            self.finished.emit(summary)
        except Exception as exc:  # pragma: no cover - UI only
            self.error.emit(str(exc))


class SettingsPage(QWidget):
    def __init__(self, state: AppState, parent=None) -> None:
        super().__init__(parent)
        self.state = state

        self.data_dir_label = QLabel("")
        self.open_data_button = QPushButton("Open data folder")

        self.encryption_label = QLabel("Encryption is required for all profiles.")

        self.clinical_checkbox = QCheckBox("Enable clinical-style insights (opt-in)")
        self.clinical_checkbox.setChecked(self.state.settings.opt_in_categories.get("clinical", False))

        self.pgx_checkbox = QCheckBox("Enable pharmacogenomics insights (opt-in)")
        self.pgx_checkbox.setChecked(self.state.settings.opt_in_categories.get("pgx", False))

        self.import_clinvar_button = QPushButton("Import ClinVar snapshot (VCF/VCF.GZ)")
        self.clinvar_status_label = QLabel("")

        self.kb_label = QLabel(f"Knowledge base version: {self.state.manifest.kb_version}")
        self.banner = QLabel("Educational only. Not medical advice. Confirm health-related findings clinically.")

        layout = QVBoxLayout()
        layout.addWidget(QLabel("Data directory"))
        layout.addWidget(self.data_dir_label)
        layout.addWidget(self.open_data_button)
        layout.addWidget(self.encryption_label)
        layout.addWidget(self.clinical_checkbox)
        layout.addWidget(self.pgx_checkbox)
        layout.addWidget(self.import_clinvar_button)
        layout.addWidget(self.clinvar_status_label)
        layout.addWidget(self.kb_label)
        layout.addWidget(self.banner)
        layout.addStretch()
        self.setLayout(layout)

        self.open_data_button.clicked.connect(self._open_data_dir)
        self.clinical_checkbox.toggled.connect(self._toggle_opt_in)
        self.pgx_checkbox.toggled.connect(self._toggle_opt_in)
        self.import_clinvar_button.clicked.connect(self._import_clinvar)
        self.state.data_changed.connect(self._refresh_clinvar_status)

        self.refresh()
        self._refresh_clinvar_status()

    def refresh(self) -> None:
        data_dir = resolve_data_dir(self.state.settings)
        self.data_dir_label.setText(str(data_dir))

    def _open_data_dir(self) -> None:
        data_dir = resolve_data_dir(self.state.settings)
        QDesktopServices.openUrl(QUrl.fromLocalFile(str(data_dir)))

    def _toggle_opt_in(self) -> None:
        if self.clinical_checkbox.isChecked() and not self.state.settings.opt_in_categories.get("clinical", False):
            confirm = QMessageBox.question(
                self,
                "Enable clinical insights",
                "Clinical insights are informational only and can be wrong. "
                "SNP chips can produce false positives. Confirm clinically before acting. "
                "Enable anyway?",
            )
            if confirm != QMessageBox.StandardButton.Yes:
                self.clinical_checkbox.setChecked(False)
                return

        self.state.settings.opt_in_categories["clinical"] = self.clinical_checkbox.isChecked()
        self.state.settings.opt_in_categories["pgx"] = self.pgx_checkbox.isChecked()
        save_settings(self.state.settings)
        self.state.data_changed.emit()

    def _import_clinvar(self) -> None:
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Select ClinVar VCF snapshot",
            "",
            "ClinVar (*.vcf *.vcf.gz *.gz)",
        )
        if not file_path:
            return

        confirm = QMessageBox.question(
            self,
            "Replace ClinVar snapshot",
            "Importing a snapshot will replace the bundled ClinVar data. Continue?",
        )
        if confirm != QMessageBox.StandardButton.Yes:
            return

        progress = QProgressDialog("Importing ClinVar snapshot...", "Cancel", 0, 0, self)
        progress.setWindowTitle("ClinVar Import")
        progress.setAutoClose(False)
        progress.setCancelButton(None)
        progress.show()

        thread = QThread(self)
        worker = ClinVarImportWorker(self.state.db_path, Path(file_path))
        worker.moveToThread(thread)
        thread.started.connect(worker.run)
        worker.progress.connect(lambda count: progress.setLabelText(f"Processed {count} variants..."))
        worker.finished.connect(lambda summary: self._finish_clinvar(summary, progress, thread, worker))
        worker.error.connect(lambda message: self._fail_clinvar(message, progress, thread, worker))
        thread.start()

    def _finish_clinvar(self, summary: dict, progress, thread, worker) -> None:
        progress.close()
        thread.quit()
        thread.wait()
        worker.deleteLater()
        thread.deleteLater()
        QMessageBox.information(
            self,
            "ClinVar import",
            f"Imported {summary.get('variant_count', 0)} high-confidence variants.",
        )
        self.state.data_changed.emit()

    def _fail_clinvar(self, message: str, progress, thread, worker) -> None:
        progress.close()
        thread.quit()
        thread.wait()
        worker.deleteLater()
        thread.deleteLater()
        QMessageBox.critical(self, "ClinVar import failed", message)

    def _refresh_clinvar_status(self) -> None:
        seed_meta = seed_metadata()
        meta = self.state.db.get_latest_clinvar_import()
        if not meta:
            self.clinvar_status_label.setText(
                f"ClinVar snapshot: bundled ({seed_meta.get('variant_count', 0)} variants)."
            )
            return
        if meta.get("file_hash_sha256") == seed_meta.get("file_hash_sha256"):
            self.clinvar_status_label.setText(
                f"ClinVar snapshot: bundled ({meta.get('variant_count', 0)} variants)."
            )
            return
        self.clinvar_status_label.setText(
            f"ClinVar snapshot imported {meta.get('imported_at', '')} "
            f"({meta.get('variant_count', 0)} variants)."
        )
