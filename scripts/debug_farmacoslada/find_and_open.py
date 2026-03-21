from sqlalchemy import text
from db_models import get_engine

engine = get_engine()
with engine.connect() as con:
    res = con.execute(text("SELECT nombre FROM productos WHERE farmacia ILIKE '%farmacoslada%' AND TRIM(nombre) != '' LIMIT 1")).fetchone()
    print("Producto:", res[0] if res else "None")
