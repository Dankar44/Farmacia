import re
import os
import sys
import logging
import argparse
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
import requests

# Directorio raíz
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from db_models import get_engine, Producto, Precio, Base
from sqlalchemy.orm import sessionmaker

FARMACIA_NOMBRE = 'Farmacoslada'

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def get_product_urls_from_sitemap():
    """
    Downloads the sitemap index, finds the product sitemaps, 
    and returns a list of valid product URLs with their EANs.
    """
    logger.info(f"[*] Fetching sitemap for {FARMACIA_NOMBRE}...")
    sitemap_url = "https://farmacoslada.com/1_es_0_sitemap.xml"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    try:
        response = requests.get(sitemap_url, headers=headers, timeout=15)
        response.raise_for_status()
    except Exception as e:
        logger.error(f"[!] Error fetching sitemap: {e}")
        return []

    namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    urls_with_ean = []
    
    try:
        root = ET.fromstring(response.content)
        for url_xml in root.findall('ns:url', namespace):
            loc = url_xml.find('ns:loc', namespace)
            if loc is not None:
                url = loc.text.strip()
                # En prestashop el EAN viene al final: -[EAN].html
                ean_match = re.search(r'-(\d{13})\.html$', url)
                if ean_match:
                    ean = ean_match.group(1)
                    urls_with_ean.append((url, ean))
    except Exception as e:
        logger.error(f"[!] Error parsing sitemap XML: {e}")

    logger.info(f"[*] Found {len(urls_with_ean)} valid product URLs with EAN.")
    return urls_with_ean


async def fetch_and_parse(url, ean, session, sem):
    """
    Asynchronously downloads and parses a single product page.
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    async with sem:
        try:
            async with session.get(url, headers=headers, timeout=15) as response:
                if response.status == 404:
                    return None
                response.raise_for_status()
                html = await response.text()
        except Exception:
            return None

    try:
        # Avoid extensive parsing if not needed, but BS4 is fine for 3000 pages synchronously in parallel chunks
        soup = BeautifulSoup(html, 'html.parser')
        
        name_tag = soup.find('h1')
        if not name_tag:
            name_tag = soup.find('title')
            if not name_tag:
                return None
        
        nombre = name_tag.text.replace('Farmacia Coslada', '').replace('Comprar', '').strip(' -|')

        precio = None
        # Intenta coger el de meta tag
        price_meta = soup.find('meta', property='product:price:amount')
        if price_meta and price_meta.get('content'):
            try:
                precio = float(price_meta['content'])
            except ValueError:
                pass
                
        if precio is None:
            price_span = soup.find('span', itemprop='price')
            if price_span and price_span.get('content'):
                try:
                    precio = float(price_span['content'])
                except ValueError:
                    pass
                    
        if precio is None:
            return None

        en_stock = False
        add_to_cart_btn = soup.find('button', class_=re.compile(r'add-to-cart'))
        if add_to_cart_btn and 'disabled' not in add_to_cart_btn.attrs:
            en_stock = True
            
        availability_meta = soup.find('link', itemprop='availability')
        if availability_meta and 'InStock' in availability_meta.get('href', ''):
            en_stock = True

        return {
            'url': url,
            'nombre': nombre,
            'ean': ean,
            'precio': precio,
            'precio_original': None,
            'en_stock': en_stock,
            'categoria': ''
        }
    except Exception:
        return None

async def run_async_scraping(products_to_scrape):
    """
    Manages the concurrent fetching of all product URLs.
    """
    results = []
    # Usar un semáforo para limitar las conexiones simultáneas (ej. 40 a la vez)
    sem = asyncio.Semaphore(40)
    
    timeout = aiohttp.ClientTimeout(total=30)
    connector = aiohttp.TCPConnector(limit=40)
    
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        tasks = []
        for url, ean in products_to_scrape:
            tasks.append(asyncio.create_task(fetch_and_parse(url, ean, session, sem)))
        
        logger.info(f"[*] Dispatching {len(tasks)} concurrent tasks. Please wait...")
        
        # Recolectar resultados con barra de progreso manual rápida
        total = len(tasks)
        completed = 0
        for f in asyncio.as_completed(tasks):
            res = await f
            completed += 1
            if completed % 100 == 0 or completed == total:
                logger.info(f"  -> Fetched {completed}/{total} pages")
            if res:
                results.append(res)
                
    return results

def ejecutar_scraping(db, limit=0):
    logger.info(f"=== Starting ASYNC scraper for {FARMACIA_NOMBRE} ===")
    
    start_time = datetime.now()
    
    products_to_scrape = get_product_urls_from_sitemap()
    
    if limit > 0:
        products_to_scrape = products_to_scrape[:limit]
        
    if not products_to_scrape:
        logger.error("[!] No products to scrape.")
        return

    # 1. Fetch data concurrently
    # Corre el event loop asíncrono desde el script secuencial
    try:
        parsed_data = asyncio.run(run_async_scraping(products_to_scrape))
    except Exception as e:
        logger.error(f"[!] Fatal error during async fetching: {e}")
        return

    logger.info(f"[*] Successfully parsed {len(parsed_data)} products. Saving to DB...")

    # 2. Save data sequentially to avoid SQLite/PostgreSQL locks
    exitos = 0
    hoy = datetime.now(timezone.utc)
    
    # Cargar URLs existentes masivamente para optimizar
    existing_products_tuples = db.query(Producto.url, Producto.id).filter_by(farmacia=FARMACIA_NOMBRE).all()
    existing_url_to_id = {u: pid for u, pid in existing_products_tuples}
    
    for datos in parsed_data:
        prod_id = existing_url_to_id.get(datos["url"])
        
        if not prod_id:
            producto = Producto(
                nombre=datos["nombre"],
                url=datos["url"],
                farmacia=FARMACIA_NOMBRE,
                categoria=datos["categoria"],
                ean=datos["ean"],
            )
            db.add(producto)
            db.flush() # need to get the ID
            prod_id = producto.id
            existing_url_to_id[datos["url"]] = prod_id
        else:
            # Note: For maximum speed we skip updating 'nombre' every time if it already exists, 
            # unless we explicitly want to query and update the row. Let's do a fast update if needed.
            db.query(Producto).filter(Producto.id == prod_id).update({
                "nombre": datos["nombre"],
                "ean": datos["ean"]
            })
            
        precio_record = Precio(
            producto_id=prod_id,
            precio=datos["precio"],
            precio_original=datos["precio_original"],
            en_stock=datos["en_stock"],
            fecha_captura=hoy,
        )
        db.add(precio_record)
        exitos += 1
        
        if exitos % 500 == 0:
            db.commit()

    db.commit()
    end_time = datetime.now()
    elapsed = (end_time - start_time).total_seconds()
    
    logger.info(f"=== Finished {FARMACIA_NOMBRE} in {elapsed:.1f} seconds ===")
    logger.info(f"Total procesados y guardados: {exitos}")
    logger.info(f"Velocidad media: {exitos/elapsed:.1f} productos/segundo")

def main():
    parser = argparse.ArgumentParser(description="Scraper Asíncrono de Farmacoslada")
    parser.add_argument("--limit", type=int, default=0, help="Limitar a N productos (0=todos)")
    args = parser.parse_args()

    engine = get_engine()
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)
    db = SessionLocal()

    try:
        ejecutar_scraping(db, limit=args.limit)
    finally:
        db.close()

if __name__ == "__main__":
    # Fix bug Windows asyncio loop EventLoopPolicy
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    main()
