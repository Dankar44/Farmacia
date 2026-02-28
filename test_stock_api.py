import sys
import os
import json
import logging

# Configurar logging para ver mensajes si los hay
logging.basicConfig(level=logging.INFO)

# Añadir el raíz del proyecto al path
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

from scrapers.farmaciasdirect import consultar_empathy, extraer_datos_producto

# Consultar el producto específico
query = "Regaxidil 50 mg/ml Minoxidil Solución Cutánea 4 Frascos de 60 ml"
print(f"Consultando la API con: '{query}'")

resultado = consultar_empathy(query, rows=10)

if resultado and 'catalog' in resultado and 'content' in resultado['catalog']:
    hits = resultado['catalog']['content']
    print(f"\nSe encontraron {len(hits)} resultados.")
    
    for i, hit in enumerate(hits):
        # Datos crudos de la API
        nombre_crudo = hit.get('__name', hit.get('nombre', 'Sin nombre'))
        dispo_cruda = hit.get('disponibilidad', 'NO_EXISTE_EL_CAMPO')
        link_crudo = hit.get('__url', hit.get('link', 'Sin link'))
        
        # Datos procesados por el scraper (cómo lo estamos interpretando ahora)
        datos_scraper = extraer_datos_producto(hit)
        en_stock_scraper = datos_scraper['en_stock']
        
        print(f"\n--- Resultado {i+1} ---")
        print(f"Nombre en API:    {nombre_crudo}")
        print(f"URL:              {link_crudo}")
        print(f"DISPONIBILIDAD (Bruto API): '{dispo_cruda}'")
        print(f"¿Stock en App?:   {en_stock_scraper}")
else:
    print("\nNo se encontraron resultados en la API para esta búsqueda.")
