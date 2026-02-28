import os
import sys
from sqlalchemy import text
import time

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
from db_models import get_engine

def consolidar_en_db():
    engine = get_engine()
    
    # Scripts SQL para preparar la data.
    # Usaremos sentencias separadas para evitar problemas de parseo.
    with engine.begin() as con:
        print("Borrando tabla anterior si existe...")
        con.execute(text("DROP TABLE IF EXISTS productos_consolidados;"))
        
        # Eliminar vistas temporales previas si quedaron colgadas en la sesión
        con.execute(text("DROP TABLE IF EXISTS ultimos_precios;"))
        con.execute(text("DROP TABLE IF EXISTS productos_con_clave;"))
        con.execute(text("DROP TABLE IF EXISTS productos_listos;"))

        print("Paso 1: Extrayendo precios vigentes...")
        con.execute(text("""
            CREATE TEMP TABLE ultimos_precios AS
            SELECT 
                p.id as producto_id, p.farmacia, p.nombre, p.url, p.ean,
                pr.precio, pr.en_stock
            FROM productos p
            JOIN precios pr ON p.id = pr.producto_id
            WHERE pr.id = (
                SELECT pr2.id FROM precios pr2
                WHERE pr2.producto_id = p.id
                ORDER BY pr2.fecha_captura DESC
                LIMIT 1
            );
        """))
        
        print("Paso 2: Generando claves de cruce EAN y Nombres...")
        # No usaremos unaccent si no está instalada, simplificamos con REPLACE y LOWER
        con.execute(text("""
            CREATE TEMP TABLE productos_con_clave AS
            SELECT 
                *,
                CASE 
                    WHEN ean IS NOT NULL AND ean ~ '^[0-9]+$' AND length(ean) BETWEEN 7 AND 14 THEN ean
                    ELSE NULL
                END as ean_valido,
                regexp_replace(lower(nombre), '[^a-z0-9]', ' ', 'g') as nombre_norm
            FROM ultimos_precios;
        """))
        
        con.execute(text("""
            CREATE TEMP TABLE productos_listos AS
            SELECT 
                *,
                COALESCE(ean_valido, 'NAME_' || trim(regexp_replace(nombre_norm, '\s+', ' ', 'g'))) as match_key
            FROM productos_con_clave;
        """))
        
        print("Paso 3: Creando tabla de consolidación final (Pivot)...")
        con.execute(text("""
            CREATE TABLE productos_consolidados AS
            SELECT 
                match_key,
                MAX(nombre) as nombre_referencia,
                MAX(ean_valido) as ean_referencia,
                
                -- DOSFARMA
                MIN(CASE WHEN farmacia = 'DosFarma' AND en_stock = true THEN precio END) as dosfarma_precio,
                MAX(CASE WHEN farmacia = 'DosFarma' THEN url END) as dosfarma_url,
                BOOL_OR(CASE WHEN farmacia = 'DosFarma' THEN en_stock ELSE false END) as dosfarma_stock,
                
                -- FARMACIASDIRECT
                MIN(CASE WHEN farmacia = 'FarmaciasDirect' AND en_stock = true THEN precio END) as farmaciasdirect_precio,
                MAX(CASE WHEN farmacia = 'FarmaciasDirect' THEN url END) as farmaciasdirect_url,
                BOOL_OR(CASE WHEN farmacia = 'FarmaciasDirect' THEN en_stock ELSE false END) as farmaciasdirect_stock,
                
                -- PROMOFARMA
                MIN(CASE WHEN farmacia = 'PromoFarma' AND en_stock = true THEN precio END) as promofarma_precio,
                MAX(CASE WHEN farmacia = 'PromoFarma' THEN url END) as promofarma_url,
                BOOL_OR(CASE WHEN farmacia = 'PromoFarma' THEN en_stock ELSE false END) as promofarma_stock,
                
                -- ATIDA
                MIN(CASE WHEN farmacia = 'Atida' AND en_stock = true THEN precio END) as atida_precio,
                MAX(CASE WHEN farmacia = 'Atida' THEN url END) as atida_url,
                BOOL_OR(CASE WHEN farmacia = 'Atida' THEN en_stock ELSE false END) as atida_stock,
                
                -- FARMACIASVAZQUEZ
                MIN(CASE WHEN farmacia = 'FarmaciasVazquez' AND en_stock = true THEN precio END) as vazquez_precio,
                MAX(CASE WHEN farmacia = 'FarmaciasVazquez' THEN url END) as vazquez_url,
                BOOL_OR(CASE WHEN farmacia = 'FarmaciasVazquez' THEN en_stock ELSE false END) as vazquez_stock
                
            FROM productos_listos
            GROUP BY match_key;
        """))
        
        print("Paso 4: Calculando estadísticas (Prec. Mín, Max, Ahorro)...")
        con.execute(text("""
            ALTER TABLE productos_consolidados 
            ADD COLUMN precio_minimo DECIMAL(10,2),
            ADD COLUMN precio_maximo DECIMAL(10,2),
            ADD COLUMN ahorro DECIMAL(10,2);
        """))
        
        con.execute(text("""
            UPDATE productos_consolidados
            SET precio_minimo = LEAST(
                    NULLIF(dosfarma_precio, NULL), 
                    NULLIF(farmaciasdirect_precio, NULL), 
                    NULLIF(promofarma_precio, NULL), 
                    NULLIF(atida_precio, NULL), 
                    NULLIF(vazquez_precio, NULL)
                ),
                precio_maximo = GREATEST(
                    COALESCE(dosfarma_precio, 0), 
                    COALESCE(farmaciasdirect_precio, 0), 
                    COALESCE(promofarma_precio, 0), 
                    COALESCE(atida_precio, 0), 
                    COALESCE(vazquez_precio, 0)
                );
        """))
        
        con.execute(text("""
            UPDATE productos_consolidados
            SET ahorro = precio_maximo - precio_minimo
            WHERE precio_minimo IS NOT NULL AND precio_maximo IS NOT NULL;
        """))
        
    print("¡Proceso de base de datos COMPLETADO!")

if __name__ == "__main__":
    start = time.time()
    consolidar_en_db()
    print(f"Todo el proceso tardó: {time.time() - start:.2f} segundos.")
