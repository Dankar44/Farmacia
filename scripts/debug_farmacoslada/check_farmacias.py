from sqlalchemy import text
from db_models import get_engine

engine = get_engine()
with engine.connect() as con:
    res = con.execute(text("SELECT farmacia, COUNT(*) FROM productos GROUP BY farmacia")).fetchall()
    print("Farmacias en DB:")
    for row in res:
        print(f"- {row[0]}: {row[1]} productos")
