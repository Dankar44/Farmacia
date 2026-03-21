"""
Script inteligente para vincular EANs perdidos en la base de datos Farmasearch.

Estrategia:
1. Buscar todos los productos que NO tienen EAN en cualquier farmacia (ej. Farmacoslada, DosFarma).
2. Intentar buscar su nombre exacto (o casi exacto) en la tabla `cima_medicamentos`.
   - Si hace match, copiamos el CN -> EAN.
3. Si no hace match en CIMA, buscar su nombre en los diccionarios maestros locales:
   - PromoFarma
   - FarmaciasVazquez
   que sí tienen EANs fiables. Si hay coincidencia de nombre, se copia el EAN al producto huérfano.

Uso:
  python scripts/vincular_diccionario.py
"""

import os
import sys
import logging
from sqlalchemy.orm import sessionmaker
from sqlalchemy import or_, and_, text

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
from db_models import get_engine, Producto, CimaMedicamento, Base

# Logging setup
LOG_DIR = os.path.join(PROJECT_ROOT, 'logs')
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'vincular_diccionario.log'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def normalizar_nombre(nombre):
    """Limpia el nombre para maximizar matches (quita ml, g, acentos...)"""
    import unicodedata
    import re
    if not nombre:
        return ""
    # Quitar acentos
    n = ''.join(c for c in unicodedata.normalize('NFD', nombre) if unicodedata.category(c) != 'Mn')
    n = n.lower().strip()
    
    # Quitar guiones, comas, puntos
    n = re.sub(r'[,\.\-]', ' ', n)
    
    # Extraer formato común
    n = re.sub(r'\b(ml|g|mg|mcg|comprimidos|capsulas|sobres)\b', '', n)
    n = re.sub(r'\s+', ' ', n).strip()
    
    return n

def ejecutar_vinculacion():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    db = Session()
    
    logger.info("="*50)
    logger.info("INICIANDO VINCULACIÓN INTELIGENTE DE EAN")
    logger.info("="*50)

    # 1. Obtener todos los productos huérfanos (sin EAN o ean muy corto)
    huerfanos = db.query(Producto).filter(
        or_(
            Producto.ean == None,
            Producto.ean == '',
            text('length(ean) < 6')
        )
    ).all()
    
    logger.info(f"Se han encontrado {len(huerfanos)} productos huérfanos sin EAN.")
    if not huerfanos:
        db.close()
        return

    # 2. Cargar diccionarios maestros en memoria
    logger.info("Cargando diccionarios maestros en memoria para cruces ultra-rápidos...")
    
    # Diccionario CIMA
    cima_all = db.query(CimaMedicamento.nombre, CimaMedicamento.cn).all()
    dict_cima = {}
    for nombre, cn in cima_all:
        norm = normalizar_nombre(nombre)
        dict_cima[norm] = cn # Preferimos el último si hay colisión, o el primero.

    # Diccionario PromoFarma / Vazquez (Nuestros maestros de Parafarmacia)
    maestros_db = db.query(Producto.nombre, Producto.ean).filter(
        Producto.farmacia.in_(['PromoFarma', 'FarmaciasVazquez']),
        Producto.ean != None,
        Producto.ean != '',
        text('length(ean) >= 6')
    ).all()
    
    dict_maestros = {}
    for nombre, ean in maestros_db:
        if nombre and ean:
            norm = normalizar_nombre(nombre)
            dict_maestros[norm] = ean

    logger.info(f" Diccionario CIMA: {len(dict_cima)} entradas norm.")
    logger.info(f" Diccionario PromoFarma/Vazquez: {len(dict_maestros)} entradas norm.")

    # 3. Procesar huérfanos
    vinculados_cima = 0
    vinculados_maestro = 0

    lote_size = 500
    modificados = 0

    for index, p in enumerate(huerfanos):
        norm_huerfano = normalizar_nombre(p.nombre)
        if not norm_huerfano: continue

        encontrado = False

        # Intento 1: Match Exacto en Maestro (promo/vazquez)
        if norm_huerfano in dict_maestros:
            p.ean = dict_maestros[norm_huerfano]
            vinculados_maestro += 1
            modificados += 1
            encontrado = True
            
        # Intento 2: Match Exacto en CIMA
        elif norm_huerfano in dict_cima:
            p.ean = dict_cima[norm_huerfano]
            vinculados_cima += 1
            modificados += 1
            encontrado = True
            
        # Guardar en lotes si hubo modificación
        if modificados >= lote_size:
            db.commit()
            modificados = 0

    if modificados > 0:
        db.commit()

    logger.info("="*50)
    logger.info("VINCULACIÓN COMPLETADA")
    logger.info("="*50)
    logger.info(f"Asignados vía Maestro (Promo/Vazquez): {vinculados_maestro}")
    logger.info(f"Asignados vía CIMA Oficial: {vinculados_cima}")
    logger.info(f"EANs Rescatados Total: {vinculados_maestro + vinculados_cima} / {len(huerfanos)}")

    db.close()

if __name__ == "__main__":
    ejecutar_vinculacion()
