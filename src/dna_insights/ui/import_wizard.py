from __future__ import annotations

from pathlib import Path
import traceback

import threading

from PySide6.QtCore import QObject, QThread, Qt, Signal, Slot
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QFileDialog,
    QFrame,
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
from dna_insights.core.clinvar import auto_import_source, import_clinvar_cache, import_clinvar_snapshot
from dna_insights.core.exceptions import ImportCancelled
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
    canceled = Signal()
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
        self._cancel_event = threading.Event()

    def request_cancel(self) -> None:
        self._cancel_event.set()

    def _cancel_check(self) -> bool:
        return self._cancel_event.is_set()

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
                cancel_check=self._cancel_check,
            )
            self.finished.emit(summary)
        except ImportCancelled:
            self.canceled.emit()
        except Exception:  # pragma: no cover - UI only
            self.error.emit(traceback.format_exc())


class ImportPage(QWidget):
    manage_profiles_requested = Signal()

    def __init__(self, state: AppState, parent=None) -> None:
        super().__init__(parent)
        self.state = state
        self._zip_member: str | None = None
        self._import_thread: QThread | None = None
        self._import_worker: ImportWorker | None = None
        self._import_progress: QProgressDialog | None = None
        self._import_cancel_button: QPushButton | None = None
        self._import_status: dict[str, object] | None = None
        self._import_done = False
        self._clinvar_thread: QThread | None = None
        self._clinvar_worker: ClinVarAutoWorker | None = None
        self._clinvar_progress: QProgressDialog | None = None
        self._clinvar_cancel_button: QPushButton | None = None
        self._last_import_ok = False

        self.profile_combo = QComboBox()
        self.file_input = QLineEdit()
        self.file_input.setReadOnly(True)
        self.browse_button = QPushButton("Browse")
        self.browse_button.setObjectName("secondaryButton")
        self.mode_combo = AutoCloseComboBox()
        self.mode_combo.addItem("Curated (recommended)", "curated")
        self.mode_combo.addItem("Full (advanced)", "full")
        self.import_button = QPushButton("Import")
        self.import_button.setObjectName("primaryButton")

        self.title_label = QLabel("Import DNA data")
        self.title_label.setObjectName("titleLabel")
        self.subtitle_label = QLabel("Stored locally. No network calls unless you opt in.")
        self.subtitle_label.setObjectName("helperLabel")

        self.manage_profiles_button = QPushButton("Manage profiles...")
        self.manage_profiles_button.setObjectName("linkButton")

        self.zip_member_label = QLabel("")
        self.zip_member_label.setObjectName("helperLabel")
        self.zip_member_label.setVisible(False)

        self.advanced_toggle = QCheckBox("Show advanced options")
        self.mode_helper = QLabel("")
        self.mode_helper.setObjectName("helperLabel")

        self.status_banner = QFrame()
        self.status_banner.setObjectName("statusBanner")
        self.status_banner.setVisible(False)
        self.status_text = QLabel("")
        self.status_text.setWordWrap(True)
        status_layout = QVBoxLayout(self.status_banner)
        status_layout.setContentsMargins(12, 8, 12, 8)
        status_layout.addWidget(self.status_text)

        profile_card = QFrame()
        profile_card.setObjectName("card")
        profile_layout = QVBoxLayout(profile_card)
        profile_layout.setContentsMargins(16, 16, 16, 16)
        profile_layout.setSpacing(8)
        profile_label = QLabel("Profile")
        profile_label.setObjectName("sectionLabel")
        profile_layout.addWidget(profile_label)
        profile_layout.addWidget(self.profile_combo)
        profile_layout.addWidget(self.manage_profiles_button)

        file_card = QFrame()
        file_card.setObjectName("card")
        file_layout = QVBoxLayout(file_card)
        file_layout.setContentsMargins(16, 16, 16, 16)
        file_layout.setSpacing(8)
        file_label = QLabel("Raw data file")
        file_label.setObjectName("sectionLabel")
        file_layout.addWidget(file_label)
        file_row = QHBoxLayout()
        file_row.setSpacing(8)
        file_row.addWidget(self.file_input, 1)
        file_row.addWidget(self.browse_button)
        file_layout.addLayout(file_row)
        file_helper = QLabel("Supported: .txt or .zip from AncestryDNA")
        file_helper.setObjectName("helperLabel")
        file_layout.addWidget(file_helper)
        file_layout.addWidget(self.zip_member_label)

        options_card = QFrame()
        options_card.setObjectName("card")
        options_layout = QVBoxLayout(options_card)
        options_layout.setContentsMargins(16, 16, 16, 16)
        options_layout.setSpacing(8)
        options_label = QLabel("Mode")
        options_label.setObjectName("sectionLabel")
        options_layout.addWidget(options_label)
        options_layout.addWidget(self.advanced_toggle)
        options_layout.addWidget(self.mode_combo)
        options_layout.addWidget(self.mode_helper)

        action_row = QHBoxLayout()
        action_row.setSpacing(8)
        action_row.addWidget(self.import_button)
        action_row.addStretch()

        layout = QVBoxLayout()
        layout.setContentsMargins(24, 24, 24, 24)
        layout.setSpacing(16)
        layout.addWidget(self.title_label)
        layout.addWidget(self.subtitle_label)
        layout.addWidget(profile_card)
        layout.addWidget(file_card)
        layout.addWidget(options_card)
        layout.addLayout(action_row)
        layout.addWidget(self.status_banner)
        layout.addStretch()
        self.setLayout(layout)

        self.browse_button.clicked.connect(self._choose_file)
        self.import_button.clicked.connect(self._start_import)
        self.manage_profiles_button.clicked.connect(self._manage_profiles)
        self.advanced_toggle.toggled.connect(self._toggle_advanced)
        self.mode_combo.currentIndexChanged.connect(self._update_mode_helper)
        self.file_input.textChanged.connect(self._update_import_button_state)
        self.profile_combo.currentIndexChanged.connect(self._update_import_button_state)
        self.state.data_changed.connect(self._refresh_profiles)
        self.state.profile_changed.connect(self._sync_current_profile)

        self._refresh_profiles()
        self.mode_combo.setEnabled(False)
        self._toggle_advanced(False)
        self._update_import_button_state()

    def _refresh_profiles(self) -> None:
        current = self.state.current_profile_id
        self.profile_combo.clear()
        for profile in self.state.list_profiles():
            self.profile_combo.addItem(profile["display_name"], profile["id"])
        if current:
            index = self.profile_combo.findData(current)
            if index >= 0:
                self.profile_combo.setCurrentIndex(index)
        self._update_import_button_state()

    def _sync_current_profile(self, profile_id: str) -> None:
        index = self.profile_combo.findData(profile_id)
        if index >= 0:
            self.profile_combo.setCurrentIndex(index)
        self._update_import_button_state()

    def _manage_profiles(self) -> None:
        self.manage_profiles_requested.emit()

    def _toggle_advanced(self, checked: bool) -> None:
        self.mode_combo.setEnabled(checked)
        self.mode_combo.setVisible(checked)
        if not checked:
            self.mode_combo.setCurrentIndex(0)
        self._update_mode_helper()

    def _update_mode_helper(self) -> None:
        mode = self.mode_combo.currentData() or "curated"
        if mode == "full":
            self.mode_helper.setText("Imports all markers (slower, larger).")
        else:
            self.mode_helper.setText("Imports only markers used by modules (faster, smaller).")

    def _set_status(self, kind: str, text: str) -> None:
        if not self.status_banner:
            return
        self.status_banner.setProperty("kind", kind)
        self.status_banner.setVisible(True)
        self.status_text.setText(text)
        self.status_banner.style().unpolish(self.status_banner)
        self.status_banner.style().polish(self.status_banner)

    def _append_status(self, text: str) -> None:
        if not self.status_banner.isVisible():
            self._set_status("info", text)
            return
        current = self.status_text.text().strip()
        if current:
            self.status_text.setText(f"{current} {text}")
        else:
            self.status_text.setText(text)

    def _update_import_button_state(self) -> None:
        has_profile = self.profile_combo.currentIndex() >= 0
        has_file = bool(self.file_input.text())
        running = self._import_thread is not None and self._import_thread.isRunning()
        self.import_button.setEnabled(has_profile and has_file and not running)

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
            self.zip_member_label.setVisible(False)
            self.zip_member_label.setText("")
            if file_path.lower().endswith(".zip"):
                members = list_zip_txt_members(Path(file_path))
                if not members:
                    QMessageBox.warning(self, "Import", "Zip file does not contain a .txt file.")
                    self.file_input.setText("")
                    self.zip_member_label.setVisible(False)
                    return
                if len(members) == 1:
                    self._zip_member = members[0]
                    self.zip_member_label.setText(f"Selected in zip: {self._zip_member}")
                    self.zip_member_label.setVisible(True)
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
                    self.zip_member_label.setVisible(False)
                    return
                self._zip_member = choice
                self.zip_member_label.setText(f"Selected in zip: {self._zip_member}")
                self.zip_member_label.setVisible(True)
            self._update_import_button_state()

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
        mode = self.mode_combo.currentData() or "curated"

        progress = QProgressDialog("Importing data...", "", 0, 100, self)
        progress.setWindowTitle("Import")
        progress.setAutoClose(False)
        progress.setAutoReset(False)
        progress.setWindowModality(Qt.WindowModality.ApplicationModal)
        cancel_button = QPushButton("Cancel")
        progress.setCancelButton(cancel_button)
        progress.show()
        self._import_progress = progress
        self._import_cancel_button = cancel_button
        self._import_done = False

        self.import_button.setEnabled(False)
        self.browse_button.setEnabled(False)
        self.profile_combo.setEnabled(False)
        self.mode_combo.setEnabled(False)
        self.advanced_toggle.setEnabled(False)
        self.import_button.setText("Importing...")
        self._set_status("info", "Importing data...")

        status = {
            "count": 0,
            "stage": "Preparing raw file...",
            "eta": 0.0,
            "percent": 0,
            "visual_percent": 0,
        }
        self._import_status = status
        self._update_import_label()

        self._import_thread = QThread(self)
        self._import_worker = ImportWorker(self.state, profile_id, file_path, mode, self._zip_member)
        thread = self._import_thread
        worker = self._import_worker
        worker.moveToThread(thread)
        thread.started.connect(worker.run)
        worker.finished.connect(thread.quit)
        worker.canceled.connect(thread.quit)
        worker.error.connect(thread.quit)
        worker.finished.connect(worker.deleteLater)
        worker.canceled.connect(worker.deleteLater)
        worker.error.connect(worker.deleteLater)
        thread.finished.connect(thread.deleteLater)
        thread.finished.connect(self._cleanup_import_refs)
        thread.finished.connect(self._reenable_import_ui)
        thread.finished.connect(self._maybe_start_clinvar_after_import)
        worker.finished.connect(self._finish_import, Qt.ConnectionType.QueuedConnection)
        worker.canceled.connect(self._cancelled_import, Qt.ConnectionType.QueuedConnection)
        worker.error.connect(self._fail_import, Qt.ConnectionType.QueuedConnection)
        worker.progress.connect(self._on_import_progress, Qt.ConnectionType.QueuedConnection)
        worker.stage.connect(self._on_import_stage, Qt.ConnectionType.QueuedConnection)
        worker.detail.connect(self._on_import_detail, Qt.ConnectionType.QueuedConnection)
        progress.canceled.connect(self._cancel_import)
        cancel_button.clicked.connect(self._cancel_import)
        thread.start()

    @Slot(int)
    def _on_import_progress(self, count: int) -> None:
        if not self._import_status:
            return
        self._import_status["count"] = count
        self._update_import_label()

    @Slot(str)
    def _on_import_stage(self, stage: str) -> None:
        if not self._import_status:
            return
        self._import_status["stage"] = stage
        self._import_status["eta"] = 0.0
        if stage == "Writing genotypes...":
            self._import_status["visual_percent"] = max(self._import_status["visual_percent"], 95)
        elif stage == "Generating insights...":
            self._import_status["visual_percent"] = max(self._import_status["visual_percent"], 98)
        self._update_import_label()

    @Slot(int, int, float)
    def _on_import_detail(self, percent: int, _bytes_read: int, eta_seconds: float) -> None:
        if not self._import_status:
            return
        self._import_status["percent"] = percent
        self._import_status["eta"] = eta_seconds
        stage = self._import_status["stage"]
        if stage.startswith("Preparing"):
            visual = int(percent * 0.1)
        elif stage.startswith("Parsing"):
            visual = 10 + int(percent * 0.8)
        else:
            visual = int(self._import_status["visual_percent"])
        self._import_status["visual_percent"] = max(self._import_status["visual_percent"], min(visual, 99))
        self._update_import_label()

    def _update_import_label(self) -> None:
        if not self._import_status or not self._import_progress:
            return
        status = self._import_status
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
        self._import_progress.setLabelText(label)
        self._import_progress.setValue(int(status["visual_percent"]))

    def _finalize_import_progress(self) -> None:
        self._close_import_progress()

    def _close_import_progress(self) -> None:
        progress = self._import_progress
        self._import_progress = None
        self._import_cancel_button = None
        self._import_status = None
        self._import_done = False
        if progress:
            progress.blockSignals(True)
            progress.hide()
            progress.deleteLater()

    @Slot(object)
    def _finish_import(self, summary) -> None:
        if self._import_status:
            self._import_status["stage"] = "Import finished."
            self._import_status["visual_percent"] = 100
            self._import_status["eta"] = 0.0
        self._update_import_label()
        self._mark_import_done()
        self._set_status(
            "success",
            f"Imported {summary.qc_report.total_markers} markers. "
            f"Call rate {summary.qc_report.call_rate:.2%}.",
        )
        self.state.data_changed.emit()
        self._last_import_ok = True

    @Slot()
    def _cancelled_import(self) -> None:
        self._finalize_import_progress()
        self._last_import_ok = False
        self._set_status("info", "Import cancelled.")

    @Slot(str)
    def _fail_import(self, message: str) -> None:
        self._finalize_import_progress()
        self._last_import_ok = False
        QMessageBox.critical(self, "Import failed", message)
        self._set_status("error", "Import failed. See the error dialog for details.")

    def _cancel_import(self) -> None:
        if not self._import_worker or not self._import_thread or not self._import_thread.isRunning():
            if self._import_done:
                self._close_import_progress()
            return
        self._import_worker.request_cancel()
        if self._import_status:
            self._import_status["stage"] = "Cancelling..."
            self._import_status["eta"] = 0.0
            self._update_import_label()
        if self._import_cancel_button:
            self._import_cancel_button.setEnabled(False)

    def _maybe_start_clinvar_after_import(self) -> None:
        if self._last_import_ok:
            self._maybe_auto_import_clinvar()

    def _maybe_auto_import_clinvar(self) -> None:
        data_dir = self.state.db_path.parent
        source = auto_import_source(data_dir)
        if not source:
            return
        rsid_filter = self.state.db.get_all_rsids()
        if not rsid_filter:
            return
        checked = self.state.db.get_clinvar_checked_rsids()
        missing = rsid_filter - checked
        if not missing:
            return
        clinvar_path = source["path"]
        clinvar_kind = source["kind"]

        label_prefix = "Updating ClinVar matches..."
        if clinvar_kind == "cache":
            label_prefix = "Updating ClinVar matches (cache)..."
        progress = QProgressDialog(label_prefix, "", 0, 100, self)
        progress.setWindowTitle("ClinVar Import")
        progress.setAutoClose(False)
        progress.setAutoReset(False)
        progress.setWindowModality(Qt.WindowModality.ApplicationModal)
        cancel_button = QPushButton("Cancel")
        progress.setCancelButton(cancel_button)
        progress.show()
        self._clinvar_progress = progress
        self._clinvar_cancel_button = cancel_button

        status = {
            "count": 0,
            "percent": 0,
            "eta": 0.0,
        }

        def update_label() -> None:
            label = label_prefix
            if status["percent"]:
                label += f" — {status['percent']}%"
            if status["count"]:
                label += f" ({status['count']} variants)"
            if status["eta"] > 0:
                minutes, seconds = divmod(int(status["eta"]), 60)
                hours, minutes = divmod(minutes, 60)
                if hours:
                    eta_text = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
                else:
                    eta_text = f"{minutes:02d}:{seconds:02d}"
                label += f" — ETA {eta_text}"
            progress.setLabelText(label)
            progress.setValue(int(status["percent"]))

        def on_progress(count: int) -> None:
            status["count"] = count
            update_label()

        def on_detail(percent: int, _bytes_read: int, eta_seconds: float) -> None:
            status["percent"] = percent
            status["eta"] = eta_seconds
            update_label()

        self._clinvar_thread = QThread(self)
        self._clinvar_worker = ClinVarAutoWorker(
            self.state.db_path, clinvar_path, missing, clinvar_kind, False
        )
        thread = self._clinvar_thread
        worker = self._clinvar_worker
        worker.moveToThread(thread)
        thread.started.connect(worker.run)
        worker.finished.connect(thread.quit)
        worker.canceled.connect(thread.quit)
        worker.error.connect(thread.quit)
        worker.finished.connect(worker.deleteLater)
        worker.canceled.connect(worker.deleteLater)
        worker.error.connect(worker.deleteLater)
        thread.finished.connect(thread.deleteLater)
        thread.finished.connect(self._cleanup_clinvar_refs)
        worker.progress.connect(on_progress, Qt.ConnectionType.QueuedConnection)
        worker.detail.connect(on_detail, Qt.ConnectionType.QueuedConnection)
        worker.finished.connect(
            lambda summary: self._finish_clinvar(summary, progress), Qt.ConnectionType.QueuedConnection
        )
        worker.canceled.connect(lambda: self._cancel_clinvar(progress), Qt.ConnectionType.QueuedConnection)
        worker.error.connect(
            lambda message: self._fail_clinvar(message, progress), Qt.ConnectionType.QueuedConnection
        )
        progress.canceled.connect(self._cancel_clinvar_request)
        cancel_button.clicked.connect(self._cancel_clinvar_request)
        thread.start()

        update_label()

    def _finish_clinvar(self, summary: dict, progress) -> None:
        progress.blockSignals(True)
        progress.hide()
        progress.deleteLater()
        if summary.get("skipped"):
            return
        self._append_status(f"ClinVar matches updated ({summary.get('variant_count', 0)} variants).")
        self.state.data_changed.emit()

    def _fail_clinvar(self, message: str, progress) -> None:
        progress.blockSignals(True)
        progress.hide()
        progress.deleteLater()
        QMessageBox.warning(self, "ClinVar import failed", message)
        self._set_status("warning", "ClinVar import failed. See the warning dialog for details.")

    def _cancel_clinvar_request(self) -> None:
        if not self._clinvar_worker or not self._clinvar_thread or not self._clinvar_thread.isRunning():
            return
        self._clinvar_worker.request_cancel()
        if self._clinvar_cancel_button:
            self._clinvar_cancel_button.setEnabled(False)
        if self._clinvar_progress:
            self._clinvar_progress.setLabelText("Cancelling ClinVar import...")

    def _cancel_clinvar(self, progress) -> None:
        progress.blockSignals(True)
        progress.hide()
        progress.deleteLater()
        self._append_status("ClinVar import cancelled.")

    def _reenable_import_ui(self) -> None:
        self.import_button.setEnabled(True)
        self.import_button.setText("Import")
        self.browse_button.setEnabled(True)
        self.profile_combo.setEnabled(True)
        self.mode_combo.setEnabled(True)
        self.advanced_toggle.setEnabled(True)
        self._update_import_button_state()

    def _cleanup_import_refs(self) -> None:
        self._import_thread = None
        self._import_worker = None

    def _mark_import_done(self) -> None:
        self._import_done = True
        if self._import_cancel_button:
            self._import_cancel_button.setEnabled(True)
            self._import_cancel_button.setText("Close")
        if self._import_progress:
            self._import_progress.setLabelText("Import finished. Click Close to dismiss.")
            self._import_progress.setValue(100)

    def _cleanup_clinvar_refs(self) -> None:
        self._clinvar_thread = None
        self._clinvar_worker = None
        self._clinvar_progress = None
        self._clinvar_cancel_button = None

class ClinVarAutoWorker(QObject):
    progress = Signal(int)
    detail = Signal(int, int, float)
    finished = Signal(dict)
    canceled = Signal()
    error = Signal(str)

    def __init__(
        self, db_path: Path, file_path: Path, rsid_filter: set[str], source_kind: str, replace: bool
    ) -> None:
        super().__init__()
        self.db_path = db_path
        self.file_path = file_path
        self.rsid_filter = rsid_filter
        self.source_kind = source_kind
        self.replace = replace
        self._cancel_event = threading.Event()

    def request_cancel(self) -> None:
        self._cancel_event.set()

    def _cancel_check(self) -> bool:
        return self._cancel_event.is_set()

    def run(self) -> None:
        try:
            if self.source_kind == "cache":
                summary = import_clinvar_cache(
                    cache_path=self.file_path,
                    db_path=self.db_path,
                    on_progress=self.progress.emit,
                    on_progress_detail=self.detail.emit,
                    replace=self.replace,
                    rsid_filter=self.rsid_filter,
                    cancel_check=self._cancel_check,
                )
            else:
                summary = import_clinvar_snapshot(
                    file_path=self.file_path,
                    db_path=self.db_path,
                    on_progress=self.progress.emit,
                    on_progress_detail=self.detail.emit,
                    replace=self.replace,
                    rsid_filter=self.rsid_filter,
                    cancel_check=self._cancel_check,
                )
            self.finished.emit(summary)
        except ImportCancelled:
            self.canceled.emit()
        except Exception:  # pragma: no cover - UI only
            self.error.emit(traceback.format_exc())
