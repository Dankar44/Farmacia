from db_models import get_engine
from sqlalchemy import text

engine = get_engine()
with engine.connect() as con:
    res = con.execute(text("SELECT nombre_normalizado FROM productos WHERE farmacia = 'farmacoslada' LIMIT 1")).fetchone()
    print('Producto:', res[0] if res else 'None')
