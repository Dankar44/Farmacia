import os
import sys
import time
from sqlalchemy import text

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)
from db_models import get_engine

engine = get_engine()

# Test searching for "crema"
search_term = "%crema%"

query_count = text("""
    SELECT COUNT(DISTINCT LOWER(nombre))
    FROM productos
    WHERE LOWER(nombre) LIKE :search
""")

query_data = text("""
    WITH latest_prices AS (
        SELECT DISTINCT ON (producto_id)
            producto_id, precio, en_stock
        FROM precios
        ORDER BY producto_id, fecha_captura DESC
    ),
    product_stats AS (
        SELECT 
            LOWER(p.nombre) as key_name,
            MIN(p.nombre) as display_name,
            MIN(pr.precio) as mejor_precio,
            COUNT(p.id) as num_farmacias,
            BOOL_OR(pr.en_stock) as tiene_stock,
            MAX(p.ean) as ean
        FROM productos p
        LEFT JOIN latest_prices pr ON p.id = pr.producto_id
        WHERE LOWER(p.nombre) LIKE :search
        GROUP BY LOWER(p.nombre)
    )
    SELECT * FROM product_stats
    ORDER BY mejor_precio ASC NULLS LAST
    LIMIT 50 OFFSET 0
""")

with engine.connect() as con:
    t0 = time.time()
    count = con.execute(query_count, {"search": search_term}).scalar()
    t1 = time.time()
    print(f"Count: {count} in {t1-t0:.3f}s")
    
    t0 = time.time()
    rows = con.execute(query_data, {"search": search_term}).fetchall()
    t1 = time.time()
    print(f"Data: {len(rows)} rows in {t1-t0:.3f}s")
