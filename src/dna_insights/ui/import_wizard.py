from __future__ import annotations

from pathlib import Path

from PySide6.QtCore import QObject, QThread, Signal
from PySide6.QtWidgets import (
    QComboBox,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QProgressDialog,
    QVBoxLayout,
    QWidget,
)

from dna_insights.app_state import AppState
from dna_insights.core.clinvar import auto_import_path, import_clinvar_snapshot
from dna_insights.core.importer import import_ancestry_file
from dna_insights.core.parser import list_zip_txt_members
from dna_insights.ui.widgets import prompt_passphrase


class ImportWorker(QObject):
    progress = Signal(int)
    stage = Signal(str)
    finished = Signal(object)
    error = Signal(str)

    def __init__(
        self, state: AppState, profile_id: str, file_path: Path, mode: str, zip_member: str | None
    ) -> None:
        super().__init__()
        self.state = state
        self.profile_id = profile_id
        self.file_path = file_path
        self.mode = mode
        self.zip_member = zip_member

    def run(self) -> None:
        try:
            summary = import_ancestry_file(
                profile_id=self.profile_id,
                file_path=self.file_path,
                db_path=self.state.db_path,
                modules=self.state.modules,
                kb_version=self.state.manifest.kb_version,
                opt_in_categories=self.state.settings.opt_in_categories,
                mode=self.mode,
                zip_member=self.zip_member,
                encryption=self.state.encryption,
                on_progress=self.progress.emit,
                on_stage=self.stage.emit,
            )
            self.finished.emit(summary)
        except Exception as exc:  # pragma: no cover - UI only
            self.error.emit(str(exc))


class ImportPage(QWidget):
    def __init__(self, state: AppState, parent=None) -> None:
        super().__init__(parent)
        self.state = state
        self._zip_member: str | None = None

        self.profile_combo = QComboBox()
        self.file_input = QLineEdit()
        self.file_input.setReadOnly(True)
        self.browse_button = QPushButton("Browse")
        self.mode_combo = QComboBox()
        self.mode_combo.addItems(["curated", "full"])
        self.import_button = QPushButton("Start import")
        self.summary_label = QLabel("")

        file_row = QHBoxLayout()
        file_row.addWidget(self.file_input)
        file_row.addWidget(self.browse_button)

        layout = QVBoxLayout()
        layout.addWidget(QLabel("Profile"))
        layout.addWidget(self.profile_combo)
        layout.addWidget(QLabel("Ancestry raw data file (.txt or .zip)"))
        layout.addLayout(file_row)
        layout.addWidget(QLabel("Import mode"))
        layout.addWidget(self.mode_combo)
        layout.addWidget(self.import_button)
        layout.addWidget(self.summary_label)
        layout.addStretch()
        self.setLayout(layout)

        self.browse_button.clicked.connect(self._choose_file)
        self.import_button.clicked.connect(self._start_import)
        self.state.data_changed.connect(self._refresh_profiles)
        self.state.profile_changed.connect(self._sync_current_profile)

        self._refresh_profiles()

    def _refresh_profiles(self) -> None:
        current = self.state.current_profile_id
        self.profile_combo.clear()
        for profile in self.state.list_profiles():
            self.profile_combo.addItem(profile["display_name"], profile["id"])
        if current:
            index = self.profile_combo.findData(current)
            if index >= 0:
                self.profile_combo.setCurrentIndex(index)

    def _sync_current_profile(self, profile_id: str) -> None:
        index = self.profile_combo.findData(profile_id)
        if index >= 0:
            self.profile_combo.setCurrentIndex(index)

    def _choose_file(self) -> None:
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Select AncestryDNA raw data",
            "",
            "Raw data (*.txt *.zip)",
        )
        if file_path:
            self.file_input.setText(file_path)
            self._zip_member = None
            if file_path.lower().endswith(".zip"):
                members = list_zip_txt_members(Path(file_path))
                if not members:
                    QMessageBox.warning(self, "Import", "Zip file does not contain a .txt file.")
                    self.file_input.setText("")
                    return
                if len(members) == 1:
                    self._zip_member = members[0]
                    return
                choice, ok = QInputDialog.getItem(
                    self,
                    "Choose raw data file",
                    "Select the .txt file inside the zip:",
                    members,
                    0,
                    False,
                )
                if not ok or not choice:
                    self.file_input.setText("")
                    return
                self._zip_member = choice

    def _start_import(self) -> None:
        if self.profile_combo.currentIndex() < 0:
            QMessageBox.information(self, "Import", "Create and select a profile first.")
            return
        if not self.file_input.text():
            QMessageBox.information(self, "Import", "Choose a raw data file.")
            return

        if self.state.encryption.is_enabled() and not self.state.encryption.has_key():
            passphrase = prompt_passphrase(self, confirm=False)
            if not passphrase:
                QMessageBox.information(self, "Import", "Passphrase is required for encryption.")
                return
            self.state.encryption.unlock(passphrase)

        profile_id = self.profile_combo.currentData()
        file_path = Path(self.file_input.text())
        mode = self.mode_combo.currentText()

        progress = QProgressDialog("Importing data...", "Cancel", 0, 0, self)
        progress.setWindowTitle("Import")
        progress.setAutoClose(False)
        progress.setCancelButton(None)
        progress.show()

        status = {"count": 0, "stage": "Parsing raw data..."}

        def update_label() -> None:
            label = status["stage"]
            if status["count"]:
                label += f" ({status['count']} markers)"
            progress.setLabelText(label)

        update_label()

        thread = QThread(self)
        worker = ImportWorker(self.state, profile_id, file_path, mode, self._zip_member)
        worker.moveToThread(thread)
        thread.started.connect(worker.run)
        worker.finished.connect(lambda summary: self._finish_import(summary, progress, thread, worker))
        worker.error.connect(lambda message: self._fail_import(message, progress, thread, worker))
        worker.progress.connect(lambda count: status.update({"count": count}) or update_label())
        worker.stage.connect(lambda stage: status.update({"stage": stage}) or update_label())
        thread.start()

    def _finish_import(self, summary, progress, thread, worker) -> None:
        progress.close()
        thread.quit()
        thread.wait()
        worker.deleteLater()
        thread.deleteLater()
        self.summary_label.setText(
            f"Imported {summary.qc_report.total_markers} markers. Call rate {summary.qc_report.call_rate:.2%}."
        )
        self.state.data_changed.emit()
        self._maybe_auto_import_clinvar()

    def _fail_import(self, message: str, progress, thread, worker) -> None:
        progress.close()
        thread.quit()
        thread.wait()
        worker.deleteLater()
        thread.deleteLater()
        QMessageBox.critical(self, "Import failed", message)

    def _maybe_auto_import_clinvar(self) -> None:
        data_dir = self.state.db_path.parent
        clinvar_path = auto_import_path(data_dir)
        if not clinvar_path:
            return
        rsid_filter = self.state.db.get_all_rsids()
        if not rsid_filter:
            return

        progress = QProgressDialog("Updating ClinVar matches...", "Cancel", 0, 0, self)
        progress.setWindowTitle("ClinVar Import")
        progress.setAutoClose(False)
        progress.setCancelButton(None)
        progress.show()

        thread = QThread(self)
        worker = ClinVarAutoWorker(self.state.db_path, clinvar_path, rsid_filter)
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
        if summary.get("skipped"):
            return
        self.summary_label.setText(
            self.summary_label.text() + f" ClinVar matches updated ({summary.get('variant_count', 0)} variants)."
        )
        self.state.data_changed.emit()

    def _fail_clinvar(self, message: str, progress, thread, worker) -> None:
        progress.close()
        thread.quit()
        thread.wait()
        worker.deleteLater()
        thread.deleteLater()
        QMessageBox.warning(self, "ClinVar import failed", message)


class ClinVarAutoWorker(QObject):
    progress = Signal(int)
    finished = Signal(dict)
    error = Signal(str)

    def __init__(self, db_path: Path, file_path: Path, rsid_filter: set[str]) -> None:
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
