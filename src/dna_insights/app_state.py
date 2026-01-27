from __future__ import annotations

from pathlib import Path

from PySide6.QtCore import QObject, Signal

from dna_insights.core.db import Database
from dna_insights.core.models import KnowledgeBaseManifest, KnowledgeModule
from dna_insights.core.security import EncryptionManager
from dna_insights.core.settings import AppSettings


class AppState(QObject):
    profile_changed = Signal(str)
    data_changed = Signal()

    def __init__(
        self,
        *,
        settings: AppSettings,
        manifest: KnowledgeBaseManifest,
        modules: list[KnowledgeModule],
        db_path: Path,
        encryption: EncryptionManager,
    ) -> None:
        super().__init__()
        self.settings = settings
        self.manifest = manifest
        self.modules = modules
        self.db_path = db_path
        self.db = Database(db_path)
        self.encryption = encryption
        self.current_profile_id: str | None = None

    def close(self) -> None:
        self.db.close()

    def list_profiles(self) -> list[dict]:
        return self.db.list_profiles()

    def create_profile(self, display_name: str, notes: str | None = None) -> str:
        profile_id = self.db.create_profile(display_name, notes)
        self.data_changed.emit()
        return profile_id

    def rename_profile(self, profile_id: str, new_name: str) -> None:
        self.db.rename_profile(profile_id, new_name)
        self.data_changed.emit()

    def delete_profile(self, profile_id: str) -> None:
        self.db.delete_profile(profile_id)
        if self.current_profile_id == profile_id:
            self.current_profile_id = None
            self.profile_changed.emit("")
        self.data_changed.emit()

    def set_current_profile(self, profile_id: str | None) -> None:
        self.current_profile_id = profile_id
        self.profile_changed.emit(profile_id or "")

    def current_profile(self) -> dict | None:
        if not self.current_profile_id:
            return None
        return self.db.get_profile(self.current_profile_id)
