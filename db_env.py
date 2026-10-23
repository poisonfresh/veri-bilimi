import os
from pathlib import Path


def _load_dotenv(path: Path) -> None:
    if not path.is_file():
        raise FileNotFoundError(
            f"{path} bulunamadı. .env.example dosyasını .env olarak kopyalayıp "
            "DB_USER ve DB_PASSWORD değerlerini girin."
        )
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def load_db_config(env_path: Path | None = None) -> dict:
    path = env_path or Path(__file__).resolve().parent / ".env"
    _load_dotenv(path)
    user = os.environ.get("DB_USER", "").strip()
    password = os.environ.get("DB_PASSWORD", "").strip()
    if not user or not password:
        raise RuntimeError("DB_USER ve DB_PASSWORD .env içinde tanımlı olmalı.")
    return {
        "host": os.environ.get("DB_HOST", "localhost").strip() or "localhost",
        "port": os.environ.get("DB_PORT", "5432").strip() or "5432",
        "user": user,
        "password": password,
        "dbname": os.environ.get("DB_NAME", "term_project_db").strip() or "term_project_db",
    }
