import re
from sqlalchemy import text
from db_models import get_engine

engine = get_engine()

# Script to fix empty names using URLs
with engine.begin() as con:
    res = con.execute(text("SELECT id, url FROM productos WHERE farmacia = 'Farmacoslada' AND nombre = ''"))
    rows = res.fetchall()
    count = 0
    for r in rows:
        url = r.url
        parts = url.split('/')[-1]
        name_part = parts.replace('.html', '')
        # remove prefix numbers
        name_part = re.sub(r'^\d+-', '', name_part)
        # remove EAN suffix if present (13 digits)
        name_part = re.sub(r'-\d{13}$', '', name_part)
        name_clean = name_part.replace('-', ' ')
        
        if name_clean:
            con.execute(text("UPDATE productos SET nombre = :n, nombre_normalizado = :n2 WHERE id = :id"),
                        {"n": name_clean.title(), "n2": name_clean.lower(), "id": r.id})
            count += 1
            
    print(f"Fixed {count} products from Farmacoslada.")
