from __future__ import annotations

from pathlib import Path
import os
from dotenv import load_dotenv

BACKEND_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BACKEND_DIR.parent
FRONTEND_DIR = PROJECT_DIR / "frontend"
DATA_DIR = BACKEND_DIR / "data"
SESSION_DIR = BACKEND_DIR / "session"
ENV_CANDIDATES = [BACKEND_DIR / ".env", PROJECT_DIR / ".env"]

for env_path in ENV_CANDIDATES:
    if env_path.exists():
        load_dotenv(env_path)
        break
else:
    load_dotenv()

API_ID = int(os.getenv("API_ID", "0") or "0")
API_HASH = os.getenv("API_HASH", "")
DB_FILE = os.getenv("DB_FILE", str(DATA_DIR / "dashboard.db"))

DATA_DIR.mkdir(parents=True, exist_ok=True)
SESSION_DIR.mkdir(parents=True, exist_ok=True)


def has_telegram_credentials() -> bool:
    return API_ID > 0 and bool(API_HASH)


def normalize_phone(phone: str) -> str:
    phone = (phone or "").strip().replace(" ", "").replace("-", "")
    if phone and not phone.startswith("+"):
        phone = "+" + phone
    return phone


def get_session_name(phone: str) -> str:
    normalized = normalize_phone(phone)
    safe_phone = normalized.replace("+", "")
    return str(SESSION_DIR / f"akun_{safe_phone}")


def get_frontend_dir() -> str:
    return str(FRONTEND_DIR)
