from __future__ import annotations

from pathlib import Path

from PySide6.QtCore import QObject, QThread, QUrl, Signal
from PySide6.QtGui import QDesktopServices
from PySide6.QtWidgets import (
    QCheckBox,
    QFileDialog,
    QFrame,
    QLabel,
    QMessageBox,
    QPushButton,
    QProgressDialog,
    QVBoxLayout,
    QWidget,
)

from dna_insights.app_state import AppState
from dna_insights.core.clinvar import auto_import_source, import_clinvar_snapshot, seed_metadata
from dna_insights.core.settings import resolve_data_dir, save_settings


class ClinVarImportWorker(QObject):
    progress = Signal(int)
    finished = Signal(dict)
    error = Signal(str)

    def __init__(self, db_path: Path, file_path: Path, rsid_filter: set[str] | None) -> None:
        super().__init__()
        self.db_path = db_path
        self.file_path = file_path
        self.rsid_filter = rsid_filter

    def run(self) -> None:
        try:
            summary = import_clinvar_snapshot(
                file_path=self.file_path,
                db_path=self.db_path,
                on_progress=self.progress.emit,
                replace=True,
                rsid_filter=self.rsid_filter,
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
        self.open_data_button.setObjectName("secondaryButton")

        self.encryption_label = QLabel("Encryption is required for all profiles.")

        self.clinical_checkbox = QCheckBox("Enable clinical-style insights (opt-in)")
        self.clinical_checkbox.setChecked(self.state.settings.opt_in_categories.get("clinical", False))

        self.pgx_checkbox = QCheckBox("Enable pharmacogenomics insights (opt-in)")
        self.pgx_checkbox.setChecked(self.state.settings.opt_in_categories.get("pgx", False))

        self.import_clinvar_button = QPushButton("Import ClinVar snapshot (VCF/VCF.GZ)")
        self.auto_import_label = QLabel("")
        self.clinvar_status_label = QLabel("")

        self.kb_label = QLabel(f"Knowledge base version: {self.state.manifest.kb_version}")
        self.banner = QLabel("Educational only. Not medical advice. Confirm health-related findings clinically.")

        title_label = QLabel("Settings")
        title_label.setObjectName("titleLabel")
        helper_label = QLabel("Privacy, clinical opt-ins, and data location.")
        helper_label.setObjectName("helperLabel")

        card = QFrame()
        card.setObjectName("card")
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(16, 16, 16, 16)
        card_layout.setSpacing(12)
        card_layout.addWidget(QLabel("Data directory"))
        card_layout.addWidget(self.data_dir_label)
        card_layout.addWidget(self.open_data_button)
        card_layout.addWidget(self.encryption_label)
        card_layout.addWidget(self.clinical_checkbox)
        card_layout.addWidget(self.pgx_checkbox)
        card_layout.addWidget(self.import_clinvar_button)
        card_layout.addWidget(self.auto_import_label)
        card_layout.addWidget(self.clinvar_status_label)
        card_layout.addWidget(self.kb_label)

        layout = QVBoxLayout()
        layout.setContentsMargins(24, 24, 24, 24)
        layout.setSpacing(16)
        layout.addWidget(title_label)
        layout.addWidget(helper_label)
        layout.addWidget(card)
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
        self._refresh_auto_import_hint()

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
            "Select ClinVar snapshot",
            "",
            "ClinVar (*.vcf *.vcf.gz *.txt *.txt.gz *.gz)",
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

        rsid_filter = self.state.db.get_all_rsids()
        thread = QThread(self)
        worker = ClinVarImportWorker(self.state.db_path, Path(file_path), rsid_filter)
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
            f"Imported {summary.get('variant_count', 0)} variants.",
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

    def _refresh_auto_import_hint(self) -> None:
        data_dir = resolve_data_dir(self.state.settings)
        source = auto_import_source(data_dir)
        if source:
            label = f"Auto import source found: {source['path']}"
            if source.get("kind") == "cache":
                label = f"Auto import source found (cache): {source['path']}"
            self.auto_import_label.setText(label)
            return
        self.auto_import_label.setText(
            f"Auto import: drop a ClinVar file at {data_dir / 'clinvar'} named "
            "'variant_summary.txt.gz' or 'clinvar.vcf.gz' to auto-import on launch. "
            "For faster imports, build a cache at "
            f"{data_dir / 'clinvar' / 'clinvar_cache.sqlite3'}. "
            "You can also bundle a full ClinVar file at src/dna_insights/knowledge_base/clinvar_full/variant_summary.txt.gz."
        )
