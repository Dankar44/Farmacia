"""
Level 2 HTTP scraper for Primor (primor.eu)
Sitemap-based scraping with rotating headers. Magento 2 store with JSON-LD product data.
"""
import sys
import os
import re
import json
import logging
import argparse
from datetime import datetime, timezone
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from db_models import get_engine, Producto, Precio, Base
from sqlalchemy.orm import sessionmaker
from scrapers.http_utils import (
    fetch_with_retry, download_sitemap, extract_price,
    random_delay, get_random_headers
)

FARMACIA = "Primor"
BASE_URL = "https://www.primor.eu"
SITEMAP_URL = "https://www.primor.eu/pub/media/sitemap_es_product.xml"
BATCH_SIZE = 100

logger = logging.getLogger(FARMACIA)
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(name)s] %(message)s')


def is_product_url(url):
    """Filter for Primor product URLs. Magento pattern: /es_es/{slug}-{id}.html"""
    if '/es_es/' not in url:
        return False
    if not url.endswith('.html'):
        return False
    skip_patterns = [
        '/categoria/', '/categorias/', '/content/',
        '/customer/', '/checkout/', '/cart/',
        '/contacto', '/blog/', '/cms/',
        '/tiendas/', '/landing/', '/universo/',
    ]
    for pattern in skip_patterns:
        if pattern in url.lower():
            return False
    return True


def extract_jsonld_product(html_text):
    """Extract product data from JSON-LD script tags (Magento 2 pattern)."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html_text, 'lxml')

    for script in soup.find_all('script', type='application/ld+json'):
        try:
            data = json.loads(script.get_text())
            if isinstance(data, list):
                for item in data:
                    if item.get('@type') == 'Product':
                        return item
            elif isinstance(data, dict) and data.get('@type') == 'Product':
                return data
        except (json.JSONDecodeError, ValueError):
            continue
    return None


def extract_datalayer(html_text):
    """Extract product info from Google Tag Manager dataLayer."""
    match = re.search(r'dataLayer\.push\((\{.*?"ecommerce".*?\})\)', html_text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except (json.JSONDecodeError, ValueError):
            pass
    return None


def extract_product_data(html_text):
    """Extract all product fields from a Primor product page."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html_text, 'lxml')

    nombre = None
    precio = None
    precio_original = None
    en_stock = True
    ean = None
    categoria = ''

    # --- JSON-LD (most reliable) ---
    jsonld = extract_jsonld_product(html_text)
    if jsonld:
        nombre = jsonld.get('name')
        ean = jsonld.get('mpn') or jsonld.get('gtin13') or jsonld.get('sku')

        # Brand prefix
        brand = None
        if isinstance(jsonld.get('brand'), dict):
            brand = jsonld['brand'].get('name')
        elif isinstance(jsonld.get('brand'), str):
            brand = jsonld['brand']

        # Price from offers
        offers = jsonld.get('offers', {})
        if isinstance(offers, list):
            offers = offers[0] if offers else {}
        precio = extract_price(str(offers.get('price', '')))
        precio_original = extract_price(str(offers.get('highPrice', '')))

        # Stock
        avail = offers.get('availability', '')
        if 'OutOfStock' in avail:
            en_stock = False
        elif 'InStock' in avail:
            en_stock = True

    # --- Fallback: dataLayer ---
    if not precio:
        dl = extract_datalayer(html_text)
        if dl:
            items = dl.get('ecommerce', {}).get('items', [])
            if items:
                item = items[0]
                precio = extract_price(str(item.get('price', '')))
                if not ean:
                    ean = item.get('ean')
                if not nombre:
                    nombre = item.get('item_name')

    # --- Fallback: HTML selectors ---
    if not nombre:
        h1 = soup.select_one('h1.page-title span, h1.page-title, h1')
        nombre = h1.get_text(strip=True) if h1 else None

    if not precio:
        price_el = soup.select_one('meta[property="product:price:amount"]')
        if price_el:
            precio = extract_price(price_el.get('content'))
        if not precio:
            price_el = soup.select_one('span.price')
            if price_el:
                precio = extract_price(price_el.get_text(strip=True))

    # Original price from HTML
    if not precio_original:
        old_price = soup.select_one('.old-price .price, .price-was')
        if old_price:
            precio_original = extract_price(old_price.get_text(strip=True))

    # Stock fallback
    if soup.select_one('.out-of-stock, .unavailable'):
        en_stock = False

    # Category from breadcrumb
    breadcrumbs = soup.select('.breadcrumbs li a span, .breadcrumb a')
    if breadcrumbs:
        parts = [b.get_text(strip=True) for b in breadcrumbs]
        parts = [p for p in parts if p.lower() not in ('inicio', 'home', '')]
        if parts:
            categoria = ' > '.join(parts[:3])

    return {
        'nombre': nombre,
        'precio': precio,
        'precio_original': precio_original,
        'en_stock': en_stock,
        'ean': ean,
        'categoria': categoria,
    }


def scrape(limit=0):
    """Run the Level 2 HTTP scraper for Primor."""
    engine = get_engine()
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    db = Session()

    logger.info(f"Descargando sitemap: {SITEMAP_URL}")
    urls = download_sitemap(SITEMAP_URL)
    urls = [u for u in urls if is_product_url(u)]
    if limit > 0:
        urls = urls[:limit]
    logger.info(f"URLs de productos encontradas: {len(urls)}")

    if not urls:
        logger.warning("No se encontraron URLs de productos.")
        db.close()
        return 0

    saved = 0
    errors = 0
    nuevos = 0
    actualizados = 0

    for i, url in enumerate(urls):
        try:
            resp = fetch_with_retry(url, referer=BASE_URL)
            if not resp:
                errors += 1
                continue

            data = extract_product_data(resp.text)

            if not data['nombre'] or not data['precio']:
                errors += 1
                continue

            existing = db.query(Producto).filter_by(url=url).first()
            if existing:
                existing.nombre = data['nombre']
                if data.get('ean'):
                    existing.ean = data['ean']
                if data.get('categoria'):
                    existing.categoria = data['categoria']
                db.add(Precio(
                    producto_id=existing.id,
                    precio=data['precio'],
                    precio_original=data['precio_original'],
                    en_stock=data['en_stock'],
                    fecha_captura=datetime.now(timezone.utc)
                ))
                actualizados += 1
            else:
                prod = Producto(
                    nombre=data['nombre'],
                    url=url,
                    farmacia=FARMACIA,
                    categoria=data.get('categoria') or '',
                    ean=data.get('ean') or ''
                )
                db.add(prod)
                db.flush()
                db.add(Precio(
                    producto_id=prod.id,
                    precio=data['precio'],
                    precio_original=data['precio_original'],
                    en_stock=data['en_stock'],
                    fecha_captura=datetime.now(timezone.utc)
                ))
                nuevos += 1

            saved += 1

            if saved % BATCH_SIZE == 0:
                db.commit()

            if (i + 1) % 50 == 0:
                logger.info(f"Progreso: {i+1}/{len(urls)} procesados | {saved} guardados | {errors} errores")

            random_delay(0.5, 1.5)

        except Exception as e:
            logger.error(f"Error procesando {url}: {e}")
            errors += 1
            try:
                db.rollback()
            except Exception:
                pass

    db.commit()
    db.close()
    logger.info(f"Completado {FARMACIA}: {saved} guardados ({nuevos} nuevos, {actualizados} actualizados), {errors} errores")
    return saved


def main():
    parser = argparse.ArgumentParser(description=f"Level 2 HTTP Scraper - {FARMACIA}")
    parser.add_argument('--limit', type=int, default=0, help='Max products to scrape (0=all)')
    parser.add_argument('--level', type=int, default=2, help='Scraping level (default=2)')
    args = parser.parse_args()
    scrape(limit=args.limit)


if __name__ == '__main__':
    main()
