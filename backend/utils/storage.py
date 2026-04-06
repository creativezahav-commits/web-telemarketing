from __future__ import annotations

import json
from pathlib import Path
import config

config.DATA_DIR.mkdir(parents=True, exist_ok=True)
config.SESSION_DIR.mkdir(parents=True, exist_ok=True)


def _resolve_data_path(nama_file: str) -> Path:
    p = Path(nama_file)
    if p.is_absolute():
        return p
    if p.parts and p.parts[0] == "data":
        return config.BACKEND_DIR / p
    return config.DATA_DIR / p.name


def baca_json(nama_file: str):
    path = _resolve_data_path(nama_file)
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def tulis_json(nama_file: str, data):
    path = _resolve_data_path(nama_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
