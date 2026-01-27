from __future__ import annotations

import logging
import sys
from pathlib import Path

from PySide6.QtWidgets import QApplication, QFileDialog, QMessageBox

from dna_insights.app_state import AppState
from dna_insights.constants import APP_NAME, LOG_FILENAME
from dna_insights.core.clinvar import seed_clinvar_if_missing
from dna_insights.core.knowledge_base import load_manifest, load_modules
from dna_insights.core.security import EncryptionManager
from dna_insights.core.settings import load_settings, resolve_data_dir, save_settings
from dna_insights.ui.main_window import MainWindow
from dna_insights.ui.widgets import prompt_passphrase


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
    exit_code = app.exec()
    state.close()
    save_settings(settings)
    return exit_code
