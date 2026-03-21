"""
Descarga el catálogo completo de medicamentos desde la API de CIMA (AEMPS).
Inserta o actualiza los registros en la tabla cima_medicamentos.

Uso:
  python scripts/descargar_cima.py
"""

import os
import sys
import logging
import requests
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert

# Setup path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
from db_models import get_engine, CimaMedicamento, Base

# Configuración de Logging
LOG_DIR = os.path.join(PROJECT_ROOT, 'logs')
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'cima_scraper.log'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuración CIMA
CIMA_API_URL = "https://cima.aemps.es/cima/rest/presentaciones"
PAGE_SIZE = 200

def descargar_catalogo_cima():
    engine = get_engine()
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    db = Session()

    logger.info("Iniciando descarga del catálogo de CIMA...")
    
    # 1. Obtener el total de páginas
    try:
        r = requests.get(f"{CIMA_API_URL}?tamanioPagina={PAGE_SIZE}&pagina=1")
        r.raise_for_status()
        data = r.json()
        total_filas = data.get('totalFilas', 0)
        total_paginas = (total_filas // PAGE_SIZE) + (1 if total_filas % PAGE_SIZE > 0 else 0)
        logger.info(f"Total de presentaciones a descargar: {total_filas} ({total_paginas} páginas)")
    except Exception as e:
        logger.error(f"Error al obtener el número de páginas de CIMA: {e}")
        db.close()
        return

    insertados = 0
    actualizados = 0
    errores = 0

    for pagina in range(1, total_paginas + 1):
        # logger.info(f"Descargando página {pagina}/{total_paginas}...")
        try:
            r = requests.get(f"{CIMA_API_URL}?tamanioPagina={PAGE_SIZE}&pagina={pagina}", timeout=15)
            if r.status_code != 200:
                logger.warning(f"Error HTTP {r.status_code} en la página {pagina}")
                errores += 1
                continue

            resultados = r.json().get('resultados', [])
            
            # Preparar lote para inserción masiva o upsert
            for med in resultados:
                cn = med.get("cn")
                if not cn:
                    continue
                    
                nregistro = med.get("nregistro")
                nombre = med.get("nombre", "")
                pactivos = med.get("pactivos", "")
                labtitular = med.get("labtitular", "")
                cpresc = med.get("cpresc", "")
                receta = med.get("receta", False)
                
                # Obtener la foto de la caja (materialas) o forma farmacéutica si la hubiera
                url_foto = None
                fotos = med.get("fotos", [])
                for foto in fotos:
                    if foto.get("tipo") == "materialas":
                        url_foto = foto.get("url")
                        break
                
                # Obtener links a prospecto y ficha técnica
                url_prospecto = None
                url_ficha = None
                docs = med.get("docs", [])
                for doc in docs:
                    if doc.get("tipo") == 2:  # 2 es prospecto
                        url_prospecto = doc.get("urlHtml") or doc.get("url")
                    elif doc.get("tipo") == 1:  # 1 es ficha técnica
                        url_ficha = doc.get("urlHtml") or doc.get("url")

                # Valores a guardar
                med_data = {
                    "nregistro": nregistro,
                    "cn": cn,
                    "nombre": nombre,
                    "pactivos": pactivos,
                    "labtitular": labtitular,
                    "cpresc": cpresc,
                    "url_foto": url_foto,
                    "url_prospecto": url_prospecto,
                    "url_ficha": url_ficha,
                    "receta": receta,
                    "fecha_actualizacion": datetime.utcnow()
                }
                
                import time
                
                # Construir la sentencia SQLite Insert... ON CONFLICT DO UPDATE (UPSERT de SQL)
                # O en postgresql: postgresql dialect insert
                from sqlalchemy.dialects.postgresql import insert
                stmt = insert(CimaMedicamento).values(**med_data)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['cn'],
                    set_=med_data
                )
                
                result = db.execute(stmt)
                
            db.commit()
            insertados += len(resultados)
            
            if pagina % 10 == 0:
                logger.info(f"  Progreso: Página {pagina}/{total_paginas} ({(pagina/total_paginas)*100:.1f}%) | Procesados: {insertados}")

        except Exception as e:
            logger.error(f"Error procesando página {pagina}: {e}")
            errores += 1
            import time
            time.sleep(2) # Pausa si hay error

    logger.info("="*50)
    logger.info("PROCESO DE CIMA COMPLETADO")
    logger.info("="*50)
    logger.info(f"Registros escaneados/insertados: {insertados}")
    logger.info(f"Errores (Páginas perdidas): {errores}")
    
    # Cuántos reales hay en la base de datos
    total_db = db.query(CimaMedicamento).count()
    logger.info(f"Total de medicamentos actualmente en DB (CimaMedicamento): {total_db}")
    
    db.close()

if __name__ == "__main__":
    descargar_catalogo_cima()
