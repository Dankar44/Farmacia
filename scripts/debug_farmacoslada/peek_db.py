from sqlalchemy import text
from db_models import get_engine

engine = get_engine()
with engine.connect() as con:
    res = con.execute(text("SELECT id, nombre, nombre_normalizado, url, ean FROM productos WHERE farmacia = 'Farmacoslada' LIMIT 1")).fetchone()
    print("Fila Farmacoslada:", res)
