"""
Aplica scripts/migrate_smart_grouping.sql (columna nombre_normalizado + función de búsqueda).
Uso: python scripts/aplicar_migracion_busqueda.py
Requiere: PostgreSQL en marcha y credenciales en .env o por defecto (db_models).
"""
import os
import subprocess
import sys
from pathlib import Path
from typing import Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

os.chdir(PROJECT_ROOT)
from dotenv import load_dotenv

load_dotenv(PROJECT_ROOT / ".env")

from db_models import get_engine


def find_psql() -> Optional[str]:
    import shutil

    w = shutil.which("psql")
    if w:
        return w
    base = Path(r"C:\Program Files\PostgreSQL")
    if base.is_dir():
        for d in sorted(base.iterdir(), reverse=True):
            cand = d / "bin" / "psql.exe"
            if cand.is_file():
                return str(cand)
    return None


def main() -> None:
    sql_file = PROJECT_ROOT / "scripts" / "migrate_smart_grouping.sql"
    if not sql_file.is_file():
        print(f"No existe {sql_file}")
        sys.exit(1)

    psql = find_psql()
    if not psql:
        print("No se encontró psql. Instala PostgreSQL o añade psql al PATH.")
        sys.exit(1)

    engine = get_engine()
    url = engine.url
    user = url.username or "postgres"
    password = url.password or ""
    host = url.host or "127.0.0.1"
    port = str(url.port or 5432)
    db = url.database or "farmacia_scraper_db"

    env = os.environ.copy()
    env["PGPASSWORD"] = password

    cmd = [
        psql,
        "-h",
        host,
        "-p",
        port,
        "-U",
        user,
        "-d",
        db,
        "-v",
        "ON_ERROR_STOP=1",
        "-f",
        str(sql_file),
    ]
    print("Ejecutando migración con:", psql)
    r = subprocess.run(cmd, env=env)
    if r.returncode != 0:
        sys.exit(r.returncode)
    print("Migración aplicada correctamente.")


if __name__ == "__main__":
    main()
