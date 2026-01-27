from __future__ import annotations

from pathlib import Path
import traceback

from PySide6.QtCore import QObject, QThread, Qt, Signal
from PySide6.QtWidgets import (
    QComboBox,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QInputDialog,
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


class AutoCloseComboBox(QComboBox):
    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        # Close reliably on mouse selection or keyboard activation.
        self.activated.connect(self._close_popup)
        self.view().clicked.connect(lambda _index: self.hidePopup())

    def _close_popup(self, _index: int) -> None:
        if self.view().isVisible():
            self.hidePopup()


class ImportWorker(QObject):
    progress = Signal(int)
    stage = Signal(str)
    detail = Signal(int, int, float)
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
                on_progress_detail=self.detail.emit,
            )
            self.finished.emit(summary)
        except Exception:  # pragma: no cover - UI only
            self.error.emit(traceback.format_exc())


class ImportPage(QWidget):
    def __init__(self, state: AppState, parent=None) -> None:
        super().__init__(parent)
        self.state = state
        self._zip_member: str | None = None
        self._import_thread: QThread | None = None
        self._import_worker: ImportWorker | None = None
        self._import_progress: QProgressDialog | None = None
        self._clinvar_thread: QThread | None = None
        self._clinvar_worker: ClinVarAutoWorker | None = None
        self._clinvar_progress: QProgressDialog | None = None
        self._last_import_ok = False

        self.profile_combo = QComboBox()
        self.file_input = QLineEdit()
        self.file_input.setReadOnly(True)
        self.browse_button = QPushButton("Browse")
        self.mode_combo = AutoCloseComboBox()
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
        if self._import_thread is not None and self._import_thread.isRunning():
            QMessageBox.information(self, "Import", "An import is already running.")
            return

        if self.state.encryption.is_enabled() and not self.state.encryption.has_key():
            passphrase = prompt_passphrase(self, confirm=False)
            if not passphrase:
                QMessageBox.information(self, "Import", "Passphrase is required for encryption.")
                return
            try:
                ok = self.state.encryption.unlock(passphrase)
                if ok is False:
                    QMessageBox.information(self, "Import", "Incorrect passphrase.")
                    return
            except Exception as exc:  # pragma: no cover - UI only
                QMessageBox.critical(self, "Import", f"Failed to unlock encryption: {exc}")
                return

        profile_id = self.profile_combo.currentData()
        file_path = Path(self.file_input.text())
        if not file_path.exists():
            QMessageBox.information(self, "Import", "Selected file no longer exists.")
            return
        mode = self.mode_combo.currentText()

        progress = QProgressDialog("Importing data...", "", 0, 100, self)
        progress.setWindowTitle("Import")
        progress.setAutoClose(False)
        progress.setCancelButton(None)
        progress.setWindowModality(Qt.WindowModality.ApplicationModal)
        progress.show()
        self._import_progress = progress

        self.import_button.setEnabled(False)
        self.browse_button.setEnabled(False)
        self.profile_combo.setEnabled(False)
        self.mode_combo.setEnabled(False)

        status = {
            "count": 0,
            "stage": "Parsing raw data...",
            "eta": 0.0,
            "percent": 0,
            "visual_percent": 0,
        }

        def update_label() -> None:
            label = status["stage"]
            if status["visual_percent"]:
                label += f" — {status['visual_percent']}%"
            if status["count"]:
                label += f" ({status['count']} markers)"
            if status["eta"] > 0:
                minutes, seconds = divmod(int(status["eta"]), 60)
                hours, minutes = divmod(minutes, 60)
                if hours:
                    eta_text = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
                else:
                    eta_text = f"{minutes:02d}:{seconds:02d}"
                label += f" — ETA {eta_text}"
            progress.setLabelText(label)

        def on_progress(count: int) -> None:
            status["count"] = count
            update_label()

        def on_stage(stage: str) -> None:
            status["stage"] = stage
            if stage == "Writing genotypes...":
                status["visual_percent"] = max(status["visual_percent"], 95)
                progress.setValue(status["visual_percent"])
            elif stage == "Generating insights...":
                status["visual_percent"] = max(status["visual_percent"], 98)
                progress.setValue(status["visual_percent"])
            update_label()

        def on_detail(percent: int, _bytes_read: int, eta_seconds: float) -> None:
            status["percent"] = percent
            status["eta"] = eta_seconds
            status["visual_percent"] = max(status["visual_percent"], int(percent * 0.9))
            progress.setValue(status["visual_percent"])
            update_label()

        update_label()

        self._import_thread = QThread(self)
        self._import_worker = ImportWorker(self.state, profile_id, file_path, mode, self._zip_member)
        thread = self._import_thread
        worker = self._import_worker
        worker.moveToThread(thread)
        thread.started.connect(worker.run)
        worker.finished.connect(thread.quit)
        worker.error.connect(thread.quit)
        worker.finished.connect(worker.deleteLater)
        worker.error.connect(worker.deleteLater)
        thread.finished.connect(thread.deleteLater)
        thread.finished.connect(self._cleanup_import_refs)
        thread.finished.connect(self._reenable_import_ui)
        thread.finished.connect(self._maybe_start_clinvar_after_import)
        worker.finished.connect(lambda summary: self._finish_import(summary, progress), Qt.ConnectionType.QueuedConnection)
        worker.error.connect(lambda message: self._fail_import(message, progress), Qt.ConnectionType.QueuedConnection)
        worker.progress.connect(on_progress, Qt.ConnectionType.QueuedConnection)
        worker.stage.connect(on_stage, Qt.ConnectionType.QueuedConnection)
        worker.detail.connect(on_detail, Qt.ConnectionType.QueuedConnection)
        thread.start()

    def _finish_import(self, summary, progress) -> None:
        progress.setValue(100)
        progress.close()
        self.summary_label.setText(
            f"Imported {summary.qc_report.total_markers} markers. Call rate {summary.qc_report.call_rate:.2%}."
        )
        self.state.data_changed.emit()
        self._last_import_ok = True

    def _fail_import(self, message: str, progress) -> None:
        progress.setValue(0)
        progress.close()
        self._last_import_ok = False
        QMessageBox.critical(self, "Import failed", message)

    def _maybe_start_clinvar_after_import(self) -> None:
        if self._last_import_ok:
            self._maybe_auto_import_clinvar()

    def _maybe_auto_import_clinvar(self) -> None:
        data_dir = self.state.db_path.parent
        clinvar_path = auto_import_path(data_dir)
        if not clinvar_path:
            return
        rsid_filter = self.state.db.get_all_rsids()
        if not rsid_filter:
            return

        progress = QProgressDialog("Updating ClinVar matches...", "", 0, 0, self)
        progress.setWindowTitle("ClinVar Import")
        progress.setAutoClose(False)
        progress.setCancelButton(None)
        progress.setWindowModality(Qt.WindowModality.ApplicationModal)
        progress.show()
        self._clinvar_progress = progress

        self._clinvar_thread = QThread(self)
        self._clinvar_worker = ClinVarAutoWorker(self.state.db_path, clinvar_path, rsid_filter)
        thread = self._clinvar_thread
        worker = self._clinvar_worker
        worker.moveToThread(thread)
        thread.started.connect(worker.run)
        worker.finished.connect(thread.quit)
        worker.error.connect(thread.quit)
        worker.finished.connect(worker.deleteLater)
        worker.error.connect(worker.deleteLater)
        thread.finished.connect(thread.deleteLater)
        thread.finished.connect(self._cleanup_clinvar_refs)
        worker.progress.connect(
            lambda count: progress.setLabelText(f"Processed {count} variants..."),
            Qt.ConnectionType.QueuedConnection,
        )
        worker.finished.connect(
            lambda summary: self._finish_clinvar(summary, progress), Qt.ConnectionType.QueuedConnection
        )
        worker.error.connect(
            lambda message: self._fail_clinvar(message, progress), Qt.ConnectionType.QueuedConnection
        )
        thread.start()

    def _finish_clinvar(self, summary: dict, progress) -> None:
        progress.close()
        if summary.get("skipped"):
            return
        self.summary_label.setText(
            self.summary_label.text() + f" ClinVar matches updated ({summary.get('variant_count', 0)} variants)."
        )
        self.state.data_changed.emit()

    def _fail_clinvar(self, message: str, progress) -> None:
        progress.close()
        QMessageBox.warning(self, "ClinVar import failed", message)

    def _reenable_import_ui(self) -> None:
        self.import_button.setEnabled(True)
        self.browse_button.setEnabled(True)
        self.profile_combo.setEnabled(True)
        self.mode_combo.setEnabled(True)

    def _cleanup_import_refs(self) -> None:
        self._import_thread = None
        self._import_worker = None
        self._import_progress = None

    def _cleanup_clinvar_refs(self) -> None:
        self._clinvar_thread = None
        self._clinvar_worker = None
        self._clinvar_progress = None

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
        except Exception:  # pragma: no cover - UI only
            self.error.emit(traceback.format_exc())
