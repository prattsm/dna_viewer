from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Tuple

from pydantic import BaseModel, Field

from dna_insights.constants import APP_SLUG, CONFIG_FILENAME, DATA_DIR_ENV


class AppSettings(BaseModel):
    data_dir: str
    opt_in_categories: dict[str, bool] = Field(default_factory=lambda: {
        "clinical": False,
        "pgx": False,
    })
    encryption_enabled: bool = False
    encryption_salt: str | None = None
    app_lock_enabled: bool = False


def get_config_dir() -> Path:
    return Path.home() / f".{APP_SLUG}"


def get_config_path() -> Path:
    return get_config_dir() / CONFIG_FILENAME


def default_data_dir() -> Path:
    return get_config_dir() / "data"


def resolve_data_dir(settings: AppSettings) -> Path:
    env_value = os.environ.get(DATA_DIR_ENV)
    if env_value:
        return Path(env_value).expanduser().resolve()
    return Path(settings.data_dir).expanduser().resolve()


def load_settings() -> Tuple[AppSettings, bool]:
    config_path = get_config_path()
    if config_path.exists():
        data = json.loads(config_path.read_text())
        settings = AppSettings(**data)
        return settings, False

    settings = AppSettings(data_dir=str(default_data_dir()))
    return settings, True


def save_settings(settings: AppSettings) -> None:
    config_dir = get_config_dir()
    config_dir.mkdir(parents=True, exist_ok=True)
    config_path = get_config_path()
    config_path.write_text(settings.model_dump_json(indent=2))
