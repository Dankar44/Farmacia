"""
Aplica migration/005_auto_normalization.sql
(trigger automático + nombre_display + re-normalización).

Uso: python scripts/apply_005_normalization.py
Requiere: credenciales de DB en .env (usa SQLAlchemy, no necesita psql instalado).
"""
import os
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

os.chdir(PROJECT_ROOT)
from dotenv import load_dotenv

load_dotenv(PROJECT_ROOT / ".env")

from sqlalchemy import text
from db_models import get_engine


def split_sql_statements(sql_content: str) -> list[str]:
    """Split SQL respecting $$ dollar-quoted blocks."""
    statements = []
    current = []
    in_dollar_quote = False

    for line in sql_content.split('\n'):
        stripped = line.strip()

        # Skip pure comment lines outside statements
        if not current and (stripped.startswith('--') or not stripped):
            continue

        # Track $$ blocks
        dollar_count = line.count('$$')
        if dollar_count % 2 == 1:
            in_dollar_quote = not in_dollar_quote

        current.append(line)

        # Statement ends with ; outside a $$ block
        if stripped.endswith(';') and not in_dollar_quote:
            stmt = '\n'.join(current).strip()
            if stmt and not all(l.strip().startswith('--') or not l.strip() for l in current):
                statements.append(stmt)
            current = []

    # Any remaining content
    if current:
        stmt = '\n'.join(current).strip()
        if stmt:
            statements.append(stmt)

    return statements


def main() -> None:
    sql_file = PROJECT_ROOT / "migration" / "005_auto_normalization.sql"
    if not sql_file.is_file():
        print(f"No existe {sql_file}")
        sys.exit(1)

    sql_content = sql_file.read_text(encoding="utf-8")
    statements = split_sql_statements(sql_content)

    engine = get_engine()

    print("Ejecutando migración 005...")
    print("  - Crea columna nombre_display")
    print("  - Crea/actualiza función normalize_product_name()")
    print("  - Crea función clean_display_name()")
    print("  - Crea trigger trg_normalize_product")
    print("  - Re-normaliza todos los productos existentes")
    print()

    with engine.connect() as con:
        for i, stmt in enumerate(statements, 1):
            # Show first meaningful line
            first_line = ""
            for line in stmt.split('\n'):
                line = line.strip()
                if line and not line.startswith('--'):
                    first_line = line[:80]
                    break
            print(f"  [{i}/{len(statements)}] {first_line}...")

            try:
                con.execute(text(stmt))
                con.commit()
            except Exception as e:
                print(f"  ERROR en statement {i}: {e}")
                con.rollback()
                sys.exit(1)

    # Verify
    with engine.connect() as con:
        total = con.execute(text("SELECT COUNT(*) FROM productos")).scalar()
        with_display = con.execute(text("SELECT COUNT(*) FROM productos WHERE nombre_display IS NOT NULL")).scalar()
        with_norm = con.execute(text("SELECT COUNT(*) FROM productos WHERE nombre_normalizado IS NOT NULL")).scalar()

    print()
    print(f"Migración 005 aplicada correctamente.")
    print(f"  Productos totales:        {total:,}")
    print(f"  Con nombre_display:       {with_display:,}")
    print(f"  Con nombre_normalizado:   {with_norm:,}")
    print()
    print("Los productos nuevos se normalizarán automáticamente con el trigger.")


if __name__ == "__main__":
    main()
