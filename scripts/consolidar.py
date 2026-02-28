import os
import sys
import unicodedata
import pandas as pd
from sqlalchemy import text

# Añadir el raíz para poder importar db_models
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
from db_models import get_engine

def get_data_from_db(engine):
    query = text("""
        SELECT
            p.id, p.farmacia, p.nombre, p.url, p.ean,
            pr.precio, pr.en_stock
        FROM productos p
        JOIN precios pr ON p.id = pr.producto_id
        WHERE pr.id = (
            SELECT pr2.id FROM precios pr2
            WHERE pr2.producto_id = p.id
            ORDER BY pr2.fecha_captura DESC
            LIMIT 1
        )
    """)
    with engine.connect() as con:
        result = con.execute(query)
        # Extract rows and columns manually to avoid Pandas version compatibility issues
        columns = result.keys()
        data = result.fetchall()
        df = pd.DataFrame(data, columns=columns)
    return df



def clean_name(name):
    if not isinstance(name, str):
        return ""
    # Remove accents, lowercase, remove non-alphanumeric, collapse spaces
    n = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('utf-8').lower()
    return ' '.join(''.join(c if c.isalnum() or c.isspace() else ' ' for c in n).split())

def is_valid_ean(ean):
    if not isinstance(ean, str): return False
    ean = ean.strip()
    return ean.isdigit() and 7 <= len(ean) <= 14

def main():
    print("Conectando a base de datos...")
    engine = get_engine()
    
    print("Descargando la tabla completa a memoria (con Pandas)...")
    df = get_data_from_db(engine)
    total_records = len(df)
    print(f"Total registros obtenidos: {total_records}")
    
    if total_records == 0:
        print("No hay datos en la BBDD.")
        return
        
    print("Normalizando nombres y validando EANs...")
    # Asegurarnos de que el precio sea numérico
    df['precio'] = pd.to_numeric(df['precio'], errors='coerce')
    
    # Crear dos columnas precalculadas para agrupación
    df['nombre_norm'] = df['nombre'].apply(clean_name)
    df['ean_valido'] = df['ean'].apply(lambda x: x if is_valid_ean(x) else None)
    
    # Vamos a usar 'ean_valido' si existe, si no usamos 'nombre_norm'
    # Así agrupamos todo en una sola columna lógica (match_key)
    df['match_key'] = df['ean_valido'].fillna('NAME_' + df['nombre_norm'])
    
    print("Agrupando y cruzando Farmacias (Vectorizado)...")
    
    # Para cada registro, elegimos el mejor precio disponible si hay duplicados de la misma farmacia
    # Ordenamos por en_stock descendente y precio ascendente (primero los que tienen stock y son más baratos)
    df = df.sort_values(by=['en_stock', 'precio'], ascending=[False, True])
    
    # Nos quedamos con el mejor de cada farmacia para el mismo producto (match_key)
    best_per_pharmacy = df.drop_duplicates(subset=['match_key', 'farmacia'], keep='first')
    
    # Ahora pivotamos la tabla para que cada farmacia sea una columna
    # Queremos columnas: {farmacia}_precio, {farmacia}_url
    pivot_price = best_per_pharmacy.pivot(index='match_key', columns='farmacia', values='precio')
    pivot_price.columns = [f"{c}_Precio" for c in pivot_price.columns]
    
    pivot_url = best_per_pharmacy.pivot(index='match_key', columns='farmacia', values='url')
    pivot_url.columns = [f"{c}_URL" for c in pivot_url.columns]
    
    pivot_stock = best_per_pharmacy.pivot(index='match_key', columns='farmacia', values='en_stock')
    # Convertir True/False a "Sí"/"No" / NaN a "No"
    for col in pivot_stock.columns:
        pivot_stock[col] = pivot_stock[col].apply(lambda x: "Sí" if x is True else "No" if pd.notnull(x) else "No")
    pivot_stock.columns = [f"{c}_EnStock" for c in pivot_stock.columns]
    
    # Juntar todas las pivot tables
    consolidated = pd.concat([pivot_price, pivot_stock, pivot_url], axis=1)
    
    # Queremos mantener el nombre original más largo/representativo y el EAN si lo tenía
    # Tomamos el primer registro de cada grupo en la DB ordenada (que tenía stock/precio) para la info general
    general_info = best_per_pharmacy.drop_duplicates(subset=['match_key'], keep='first')[['match_key', 'nombre', 'ean_valido']]
    consolidated = consolidated.join(general_info.set_index('match_key'))
    
    # Filtrar solo precios válidos de las farmacias
    farmacias = ["DosFarma", "FarmaciasDirect", "PromoFarma", "Atida", "FarmaciasVazquez"]
    precio_cols = [f"{f}_Precio" for f in farmacias if f"{f}_Precio" in consolidated.columns]
    stock_cols = [f"{f}_EnStock" for f in farmacias if f"{f}_EnStock" in consolidated.columns]
    
    consolidated['Precio_Min'] = consolidated[precio_cols].min(axis=1)
    consolidated['Precio_Max'] = consolidated[precio_cols].max(axis=1)
    consolidated['Ahorro_EUR'] = consolidated['Precio_Max'] - consolidated['Precio_Min']
    consolidated['Ahorro_EUR'] = consolidated['Ahorro_EUR'].fillna(0).round(2)
    
    # Reordenar las columnas para que queden bonitas
    col_order = ['nombre', 'ean_valido']
    for f in farmacias:
        if f"{f}_Precio" in consolidated.columns:
            col_order.extend([f"{f}_Precio", f"{f}_EnStock", f"{f}_URL"])
    col_order.extend(['Precio_Min', 'Precio_Max', 'Ahorro_EUR'])
    
    # Asegurarnos de que las columnas existan, si no ignorarlas
    final_cols = [c for c in col_order if c in consolidated.columns]
    consolidated = consolidated[final_cols]
    
    # Renombrar 'nombre' y 'ean_valido'
    consolidated.rename(columns={'nombre': 'Producto', 'ean_valido': 'EAN_Principal'}, inplace=True)
    
    # Ordenar por el mayor ahorro (aquí es donde merece la pena comparar)
    consolidated.sort_values(by='Ahorro_EUR', ascending=False, inplace=True)
    
    out_file = os.path.join(PROJECT_ROOT, 'exports', 'comparativa_global_rapida.xlsx')
    print(f"Generando documento Excel ordenado con {len(consolidated)} productos agrupados.")
    
    # Generar el Excel
    with pd.ExcelWriter(out_file, engine='openpyxl') as writer:
        consolidated.to_excel(writer, index=False, sheet_name='Comparativa')
        
    print("¡FINALIZADO! Excel creado al instante.")

if __name__ == "__main__":
    main()
