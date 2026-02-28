import os
import sys
import unicodedata
import re
from sqlalchemy import text
from collections import defaultdict

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
from db_models import get_engine

def clean_name(name):
    if not isinstance(name, str):
        return ""
    n = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('utf-8').lower()
    return ' '.join(''.join(c if c.isalnum() or c.isspace() else ' ' for c in n).split())

def is_valid_ean(ean):
    if not isinstance(ean, str): return False
    ean = ean.strip()
    return ean.isdigit() and 7 <= len(ean) <= 14

def main():
    print("==================================================")
    print("   CONSOLIDADOR DE FARMACIAS (MODO POR LOTES)")
    print("==================================================")
    engine = get_engine()
    
    print("\n[1/4] Contando productos en la base de datos...")
    with engine.connect() as con:
        # Calcular el tamaño total para la barra de progreso
        count_query = text("""
            SELECT COUNT(p.id)
            FROM productos p
            JOIN precios pr ON p.id = pr.producto_id
            WHERE pr.id = (
                SELECT pr2.id FROM precios pr2
                WHERE pr2.producto_id = p.id
                ORDER BY pr2.fecha_captura DESC
                LIMIT 1
            )
        """)
        total_rows = con.execute(count_query).scalar()
        
    print(f"Total de productos encontrados: {total_rows}")
    if total_rows == 0:
        return
        
    print("\n[2/4] Extrayendo y agrupando productos en la Memoria RAM...")
    print("Esto puede tardar un poco, pero verás el progreso:")
    
    batch_size = 10000
    grupos = defaultdict(lambda: {
        'nombre_ref': '',
        'ean_ref': '',
        'precios': {},
        'urls': {},
        'stocks': {}
    })
    
    farmacias_conocidas = ["DosFarma", "FarmaciasDirect", "PromoFarma", "Atida", "FarmaciasVazquez"]
    
    with engine.connect() as con:
        # Vamos procesando por páginas usando OFFSET/LIMIT para no saturar memoria
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
            ORDER BY p.id
            LIMIT :limit OFFSET :offset
        """)
        
        offset = 0
        procesados = 0
        
        while offset < total_rows:
            result = con.execute(query, {"limit": batch_size, "offset": offset})
            rows = result.fetchall()
            
            if not rows:
                break
                
            for row in rows:
                p_id, farmacia, nombre, url, ean, precio, en_stock = row
                
                # Definir clave de match
                if is_valid_ean(ean):
                    match_key = str(ean)
                    es_ean = True
                else:
                    match_key = 'NAME_' + clean_name(nombre)
                    es_ean = False
                    
                # Guardar en diccionario
                if not grupos[match_key]['nombre_ref'] or len(str(nombre)) > len(str(grupos[match_key]['nombre_ref'])):
                    grupos[match_key]['nombre_ref'] = nombre
                if es_ean:
                    grupos[match_key]['ean_ref'] = ean
                    
                # Guardar datos de farmacia (quedándonos con el precio más bajo si hay duplis internos)
                if farmacia in farmacias_conocidas:
                    precio_actual = grupos[match_key]['precios'].get(farmacia)
                    if precio is not None:
                        precio = float(precio)
                        if precio_actual is None or (en_stock and precio < precio_actual):
                            grupos[match_key]['precios'][farmacia] = precio
                            grupos[match_key]['urls'][farmacia] = url
                            grupos[match_key]['stocks'][farmacia] = en_stock

                procesados += 1
                
            offset += batch_size
            porcentaje = (procesados / total_rows) * 100
            print(f"\r  Progreso: {procesados}/{total_rows} productos cruzados ({porcentaje:.1f}%)", end="")
            
    print("\n\n[3/4] Generando Tabla Consolidada...")
    
    # Preparar datos para importar a sqlite o excel
    productos_unicos = len(grupos)
    print(f"Tras eliminar duplicados exactos, nos quedan {productos_unicos} productos únicos en total.")
    
    # Lo pasamos a una lista de diccionarios para que sea fácil
    tabla_final = []
    
    print("Calculando diferencias de precios...")
    procesados = 0
    for match_key, datos in grupos.items():
        fila = {
            'Producto': datos['nombre_ref'],
            'EAN': datos['ean_ref'],
        }
        
        precios_validos = []
        for f in farmacias_conocidas:
            fila[f'{f}_Precio'] = datos['precios'].get(f, None)
            fila[f'{f}_EnStock'] = "Sí" if datos['stocks'].get(f, False) else "No"
            fila[f'{f}_URL'] = datos['urls'].get(f, "")
            
            if fila[f'{f}_EnStock'] == "Sí" and fila[f'{f}_Precio'] is not None:
                precios_validos.append(fila[f'{f}_Precio'])
                
        if precios_validos:
            fila['Precio_Min'] = min(precios_validos)
            fila['Precio_Max'] = max(precios_validos)
            fila['Ahorro_EUR'] = round(fila['Precio_Max'] - fila['Precio_Min'], 2)
        else:
            fila['Precio_Min'] = None
            fila['Precio_Max'] = None
            fila['Ahorro_EUR'] = 0.0
            
        tabla_final.append(fila)
        
        procesados += 1
        if procesados % 10000 == 0:
            print(f"\r  Preparando filas: {procesados}/{productos_unicos}", end="")
            
    # Ordenar por ahorro para que los que más rentan salgan primero
    tabla_final.sort(key=lambda x: x['Ahorro_EUR'] or -1, reverse=True)
            
    print("\n\n[4/4] Exportando al Excel comparativo gigante...")
    
    try:
        import pandas as pd
        df = pd.DataFrame(tabla_final)
        out_file = os.path.join(PROJECT_ROOT, 'exports', 'comparativa_global_final.xlsx')
        
        print("  Escribiendo en el archivo... (esto tarda unos 30-60 segundos por la cantidad de filas)")
        with pd.ExcelWriter(out_file, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Comparativa')
            
        print(f"\n¡ÉXITO ABSOLUTO! Excel guardado en: {out_file}")
    except Exception as e:
        print(f"\nError creando Excel: {e}")

if __name__ == "__main__":
    main()
