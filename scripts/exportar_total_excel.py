import os
import sys
import unicodedata
import pandas as pd
from sqlalchemy import text

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
from db_models import get_engine

def get_data_from_db(engine):
    # Usamos DISTINCT ON de PostgreSQL (mucho más rápido que subconsulta correlacionada)
    query = text("""
        SELECT
            p.id, p.farmacia, p.nombre, p.url, p.ean,
            pr.precio, pr.en_stock
        FROM productos p
        JOIN (
            SELECT DISTINCT ON (producto_id)
                id, producto_id, precio, en_stock
            FROM precios
            ORDER BY producto_id, fecha_captura DESC
        ) pr ON p.id = pr.producto_id
    """)
    with engine.connect() as con:
        result = con.execute(query)
        columns = result.keys()
        data = result.fetchall()
        df = pd.DataFrame(data, columns=columns)
    return df

def clean_name(name):
    if not isinstance(name, str):
        return ""
    n = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('utf-8').lower()
    return ' '.join(''.join(c if c.isalnum() or c.isspace() else ' ' for c in n).split())

def is_valid_ean(ean):
    if not isinstance(ean, str): return False
    ean = ean.strip()
    return ean.isdigit() and 7 <= len(ean) <= 14

def col_letter(col_idx):
    """Convierte índice de columna (0-based) a letra de Excel (A, B, ... Z, AA, AB...)"""
    letter = ""
    idx = col_idx + 1
    while idx > 0:
        idx, remainder = divmod(idx - 1, 26)
        letter = chr(65 + remainder) + letter
    return letter

def main():
    import time
    start_time = time.time()
    
    print("1. Conectando a la base de datos...")
    engine = get_engine()
    
    print("2. Descargando datos (esto puede tardar unos segundos si hay muchos datos)...", flush=True)
    df = get_data_from_db(engine)
    total_records = len(df)
    print(f"   -> ¡Descargados {total_records} registros de precios!")
    
    if total_records == 0:
        print("No hay datos en la base de datos.")
        return
        
    print("3. Limpiando y unificando nombres y EANs...")
    df['precio'] = pd.to_numeric(df['precio'], errors='coerce')
    df['nombre_norm'] = df['nombre'].apply(clean_name)
    df['ean_valido'] = df['ean'].apply(lambda x: x if is_valid_ean(x) else None)
    df['match_key'] = df['ean_valido'].fillna('NAME_' + df['nombre_norm'])
    
    # Quedarnos con el mejor precio si hay duplicados internos de la misma farmacia
    print("   -> Eliminando duplicados internos de las tiendas...")
    df = df.sort_values(by=['en_stock', 'precio'], ascending=[False, True])
    best_per_pharmacy = df.drop_duplicates(subset=['match_key', 'farmacia'], keep='first')
    print(f"   -> Quedan {len(best_per_pharmacy)} registros únicos aptos para cruzar.")
    
    print("4. Cruzando productos (Pivotando Farmacias)...")
    # Pivotar precios
    pivot_price = best_per_pharmacy.pivot(index='match_key', columns='farmacia', values='precio')
    pivot_price.columns = [f"{c}_Precio" for c in pivot_price.columns]
    
    # Pivotar URLs
    pivot_url = best_per_pharmacy.pivot(index='match_key', columns='farmacia', values='url')
    pivot_url.columns = [f"{c}_URL" for c in pivot_url.columns]

    # Pivotar Stock
    pivot_stock = best_per_pharmacy.pivot(index='match_key', columns='farmacia', values='en_stock')
    for col in pivot_stock.columns:
        pivot_stock[col] = pivot_stock[col].apply(lambda x: "Sí" if x is True else "No" if pd.notnull(x) else "No")
    pivot_stock.columns = [f"{c}_EnStock" for c in pivot_stock.columns]
    
    consolidated = pd.concat([pivot_price, pivot_stock, pivot_url], axis=1)
    
    # Agregar Nombre y EAN
    general_info = best_per_pharmacy.drop_duplicates(subset=['match_key'], keep='first')[['match_key', 'nombre', 'ean_valido']]
    consolidated = consolidated.join(general_info.set_index('match_key'))
    
    # Preparar el dataframe final
    farmacias = ["DosFarma", "FarmaciasDirect", "PromoFarma", "Atida", "FarmaciasVazquez"]
    precio_cols = [f"{f}_Precio" for f in farmacias if f"{f}_Precio" in consolidated.columns]
    
    # Calcular el mínimo, máximo y ahorro (igual que comparativa_global_rapida)
    consolidated['Precio_Min'] = consolidated[precio_cols].min(axis=1)
    consolidated['Precio_Max'] = consolidated[precio_cols].max(axis=1)
    consolidated['Ahorro_EUR'] = consolidated['Precio_Max'] - consolidated['Precio_Min']
    consolidated['Ahorro_EUR'] = consolidated['Ahorro_EUR'].fillna(0).round(2)
    
    # Ordenar columnas estéticamente
    col_order = ['nombre', 'ean_valido']
    for f in farmacias:
        if f"{f}_Precio" in consolidated.columns:
            # Agrupar las columnas de cada farmacia juntas: Precio, Stock, Link
            col_order.extend([f"{f}_Precio", f"{f}_EnStock", f"{f}_URL"])
            
    col_order.extend(['Precio_Min', 'Precio_Max', 'Ahorro_EUR'])

    final_cols = [c for c in col_order if c in consolidated.columns]
    consolidated = consolidated[final_cols]
    
    consolidated.rename(columns={'nombre': 'Producto', 'ean_valido': 'EAN'}, inplace=True)
    
    # Limpiar nombres: quitar espacios al inicio/final y poner solo la primera letra en mayúscula
    consolidated['Producto'] = consolidated['Producto'].str.strip().str.capitalize()
    
    # Ordenamos alfabéticamente por nombre de producto
    consolidated.sort_values(by='Producto', ascending=True, inplace=True)
    
    out_file = os.path.join(PROJECT_ROOT, 'exports', 'total.xlsx')
    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    
    print(f"5. Generando Excel con {len(consolidated)} productos agrupados finales...")
    print("   -> Escribiendo datos en el Excel con xlsxwriter (mucho más rápido)...", flush=True)
    
    with pd.ExcelWriter(out_file, engine='xlsxwriter',
                         engine_kwargs={'options': {'strings_to_urls': False}}) as writer:
        # fillna(' ') -> todas las celdas vacías llevan un espacio,
        # así Excel no permite que el texto se desborde a la celda de al lado
        consolidated.fillna(' ').to_excel(writer, index=False, sheet_name='Precios Totales')
        print("   -> Datos escritos correctamente.", flush=True)
        
        workbook = writer.book
        worksheet = writer.sheets['Precios Totales']
        
        total_filas = len(consolidated)
        cols_list = list(consolidated.columns)
        
        # Formato verde para precios mínimos
        green_fmt = workbook.add_format({'bg_color': '#C6EFCE', 'num_format': '#,##0.00'})
        
        # Encontrar las columnas de precio de cada farmacia
        precio_col_letters = []
        for i, col_name in enumerate(cols_list):
            if "_Precio" in col_name and "Min" not in col_name and "Max" not in col_name:
                precio_col_letters.append(col_letter(i))
        
        # Formato condicional: colorear verde la celda si su valor == el mínimo de los precios
        if precio_col_letters:
            print(f"   -> Aplicando formato condicional verde a {len(precio_col_letters)} columnas de precio...", flush=True)
            min_refs = ",".join([f"${l}2" for l in precio_col_letters])
            min_formula = f"MIN({min_refs})"
            
            for pl in precio_col_letters:
                col_range = f"{pl}2:{pl}{total_filas + 1}"
                worksheet.conditional_format(col_range, {
                    'type': 'formula',
                    'criteria': f'=AND({pl}2={min_formula}, {pl}2<>"")',
                    'format': green_fmt
                })
        
        # Ajustar anchos de columna
        print("   -> Aplicando anchos de columna correctos...", flush=True)
        for i, col_name in enumerate(cols_list):
            if col_name in ('Producto',):
                worksheet.set_column(i, i, 50)
            elif col_name in ('EAN',):
                worksheet.set_column(i, i, 18)
            elif "_URL" in col_name:
                worksheet.set_column(i, i, 80)
            elif "_Precio" in col_name or "Min" in col_name or "Max" in col_name or "Ahorro" in col_name:
                worksheet.set_column(i, i, 28)
            elif "_EnStock" in col_name:
                worksheet.set_column(i, i, 28)

    elapsed = time.time() - start_time
    print(f"\n¡Okey, EXCEL LISTO! Archivo guardado correctamente en:\n {out_file}")
    print(f"⏱️ Todo el proceso ha tardado {elapsed:.1f} segundos.")

if __name__ == "__main__":
    main()
