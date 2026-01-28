from __future__ import annotations

import logging
import sys
from pathlib import Path

from PySide6.QtCore import QObject, QThread, QTimer, Qt, Signal
from PySide6.QtWidgets import QApplication, QFileDialog, QMessageBox

from dna_insights.app_state import AppState
from dna_insights.constants import APP_NAME, LOG_FILENAME
from dna_insights.core.clinvar import (
    auto_import_source,
    import_clinvar_cache,
    import_clinvar_snapshot,
    seed_clinvar_if_missing,
)
from dna_insights.core.knowledge_base import load_manifest, load_modules
from dna_insights.core.security import EncryptionManager
from dna_insights.core.settings import load_settings, resolve_data_dir, save_settings
from dna_insights.ui.main_window import MainWindow
from dna_insights.ui.widgets import prompt_passphrase


class ClinVarAutoWorker(QObject):
    finished = Signal(dict)
    error = Signal(str)

    def __init__(self, db_path: Path, file_path: Path, rsid_filter: set[str], source_kind: str) -> None:
        super().__init__()
        self.db_path = db_path
        self.file_path = file_path
        self.rsid_filter = rsid_filter
        self.source_kind = source_kind

    def run(self) -> None:
        try:
            if self.source_kind == "cache":
                summary = import_clinvar_cache(
                    cache_path=self.file_path,
                    db_path=self.db_path,
                    replace=True,
                    rsid_filter=self.rsid_filter,
                )
            else:
                summary = import_clinvar_snapshot(
                    file_path=self.file_path,
                    db_path=self.db_path,
                    replace=True,
                    rsid_filter=self.rsid_filter,
                )
            self.finished.emit(summary)
        except Exception as exc:  # pragma: no cover - UI only
            self.error.emit(str(exc))


class ClinVarAutoController(QObject):
    def __init__(self, state: AppState, data_dir: Path, parent: QObject | None = None) -> None:
        super().__init__(parent)
        self.state = state
        self.data_dir = data_dir
        self.thread: QThread | None = None
        self.worker: ClinVarAutoWorker | None = None

    def start(self) -> None:
        if self.thread is not None:
            return
        source = auto_import_source(self.data_dir)
        if not source:
            return
        rsid_filter = self.state.db.get_all_rsids()
        if not rsid_filter:
            logging.info("ClinVar auto-import skipped: no rsIDs available yet.")
            return
        file_path = source["path"]
        source_kind = source["kind"]
        self.thread = QThread(self)
        self.worker = ClinVarAutoWorker(self.state.db_path, file_path, rsid_filter, source_kind)
        self.worker.moveToThread(self.thread)
        self.thread.started.connect(self.worker.run)
        self.worker.finished.connect(self._on_done, Qt.QueuedConnection)
        self.worker.error.connect(self._on_error, Qt.QueuedConnection)
        self.worker.finished.connect(self.worker.deleteLater)
        self.worker.error.connect(self.worker.deleteLater)
        self.thread.finished.connect(self.thread.deleteLater)
        self.thread.start()

    def _finalize(self) -> None:
        if self.thread:
            self.thread.quit()
        self.thread = None
        self.worker = None

    def _on_done(self, summary: dict) -> None:
        if summary.get("skipped"):
            logging.info("ClinVar auto-import skipped: %s", summary.get("reason", "unknown"))
        else:
            logging.info("ClinVar auto-imported %s variants.", summary.get("variant_count", 0))
            self.state.data_changed.emit()
        self._finalize()

    def _on_error(self, message: str) -> None:
        logging.error("ClinVar auto-import failed: %s", message)
        self._finalize()


def _setup_logging(log_dir: Path) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / LOG_FILENAME
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(log_path), logging.StreamHandler(sys.stdout)],
    )


def _maybe_choose_data_dir(app: QApplication, default_dir: Path) -> Path:
    chosen = QFileDialog.getExistingDirectory(
        None,
        "Choose a data directory for DNA Insights",
        str(default_dir),
    )
    if chosen:
        return Path(chosen).expanduser().resolve()
    return default_dir


def main() -> int:
    settings, first_run = load_settings()
    app = QApplication(sys.argv)
    app.setApplicationName(APP_NAME)

    data_dir = resolve_data_dir(settings)
    if first_run:
        data_dir = _maybe_choose_data_dir(app, data_dir)
        settings.data_dir = str(data_dir)
        save_settings(settings)

    _setup_logging(data_dir / "logs")

    manifest = load_manifest()
    modules = load_modules(manifest)
    encryption = EncryptionManager(settings)

    if settings.encryption_enabled and not settings.encryption_salt:
        passphrase = prompt_passphrase(confirm=True)
        if not passphrase:
            QMessageBox.critical(
                None,
                APP_NAME,
                "A passphrase is required to enable mandatory encryption. The app will now exit.",
            )
            return 1
        encryption.unlock(passphrase)
        save_settings(settings)

    state = AppState(
        settings=settings,
        manifest=manifest,
        modules=modules,
        db_path=data_dir / "dna_insights.sqlite3",
        encryption=encryption,
    )
    seed_clinvar_if_missing(state.db)
    window = MainWindow(state)
    window.show()

    clinvar_controller = ClinVarAutoController(state, data_dir, parent=window)
    QTimer.singleShot(0, clinvar_controller.start)
    exit_code = app.exec()
    state.close()
    save_settings(settings)
    return exit_code
