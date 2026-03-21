"""
Level 2 HTTP scraper for FarmaciasDirect (farmaciasdirect.com)
Sitemap-based scraping with rotating headers, retries, and BeautifulSoup parsing.
"""
import sys
import os
import re
import logging
import argparse
from datetime import datetime, timezone
from decimal import Decimal

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from db_models import get_engine, Producto, Precio, Base
from sqlalchemy.orm import sessionmaker
from scrapers.http_utils import (
    fetch_with_retry, download_sitemap, extract_product_from_html,
    random_delay, get_random_headers, extract_price
)

FARMACIA = "FarmaciasDirect"
BASE_URL = "https://www.farmaciasdirect.com"
SITEMAP_URL = "https://www.farmaciasdirect.com/sitemap.xml"
BATCH_SIZE = 100

HTML_CONFIG = {
    'name_selector': 'h1',
    'price_selector': 'meta[property="product:price:amount"]',
    'price_attr': 'content',
    'stock_selector': '.add-to-cart, button[type="submit"][name="add"], #add-to-cart-button',
    'ean_selector': '[itemprop="sku"]',
    'ean_attr': 'content',
}

logger = logging.getLogger(FARMACIA)
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(name)s] %(message)s')


def is_product_url(url):
    """Filter for product URLs from the sitemap."""
    skip_patterns = [
        '/categoria/', '/categorias/', '/marca/', '/blog/',
        '/info/', '/login', '/registro', '/checkout',
        '/carrito', '/contacto', '/ayuda/', '/faq/',
        '/politica', '/condiciones', '/aviso-legal',
        '/content/', '/module/', '/modulo/',
    ]
    for pattern in skip_patterns:
        if pattern in url.lower():
            return False
    if url.rstrip('/') == BASE_URL:
        return False
    # Must be deeper than just the domain
    path = url.replace(BASE_URL, '').strip('/')
    if not path or '/' not in path and not path.endswith('.html'):
        return False
    return True


def extract_price_fallback(html_text):
    """Fallback price extraction."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html_text, 'lxml')

    # Try itemprop="price"
    el = soup.select_one('[itemprop="price"]')
    if el:
        price = extract_price(el.get('content') or el.get_text(strip=True))
        if price:
            return price

    # Try common price classes
    for selector in ['.price', '.current-price', '.product-price', '.special-price']:
        el = soup.select_one(selector)
        if el:
            price = extract_price(el.get_text(strip=True))
            if price:
                return price

    # Try JSON-LD
    for script in soup.find_all('script', type='application/ld+json'):
        text = script.get_text()
        match = re.search(r'"price"\s*:\s*["\']?([\d.,]+)', text)
        if match:
            price = extract_price(match.group(1))
            if price:
                return price

    return None


def extract_ean_fallback(html_text):
    """Fallback EAN extraction from structured data."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html_text, 'lxml')

    # Try itemprop="sku" text
    el = soup.select_one('[itemprop="sku"]')
    if el:
        val = el.get('content') or el.get_text(strip=True)
        if val:
            return val

    # Try gtin13
    el = soup.select_one('[itemprop="gtin13"]')
    if el:
        val = el.get('content') or el.get_text(strip=True)
        if val:
            return val

    # Try JSON-LD
    for script in soup.find_all('script', type='application/ld+json'):
        text = script.get_text()
        gtin_match = re.search(r'"gtin13"\s*:\s*"(\d{13})"', text)
        if gtin_match:
            return gtin_match.group(1)
        sku_match = re.search(r'"sku"\s*:\s*"([^"]+)"', text)
        if sku_match:
            return sku_match.group(1)

    return None


def check_stock(html_text):
    """Check product availability."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html_text, 'lxml')
    text_lower = html_text.lower()

    # Check for out-of-stock text markers
    out_of_stock_markers = ['agotado', 'fuera de stock', 'sin stock', 'no disponible']
    for marker in out_of_stock_markers:
        if marker in text_lower:
            return False

    # Check availability schema
    avail = soup.select_one('[itemprop="availability"]')
    if avail:
        href = avail.get('href', '') or avail.get('content', '')
        if 'OutOfStock' in href:
            return False
        if 'InStock' in href:
            return True

    # Check for add-to-cart button
    cart_btn = soup.select_one('.add-to-cart, button[type="submit"][name="add"], #add-to-cart-button')
    if cart_btn:
        if cart_btn.get('disabled') is not None:
            return False
        return True

    # JSON-LD availability
    if '"outofstock"' in text_lower:
        return False
    if '"instock"' in text_lower:
        return True

    return True  # Default to in stock


def scrape(limit=0):
    """Run the Level 2 HTTP scraper for FarmaciasDirect."""
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

            data = extract_product_from_html(resp.text, BASE_URL, HTML_CONFIG)

            # Fallback price extraction
            if not data['precio']:
                data['precio'] = extract_price_fallback(resp.text)

            # Fallback EAN extraction
            if not data.get('ean'):
                data['ean'] = extract_ean_fallback(resp.text)

            # Custom stock check
            data['en_stock'] = check_stock(resp.text)

            if not data['nombre'] or not data['precio']:
                errors += 1
                continue

            existing = db.query(Producto).filter_by(url=url).first()
            if existing:
                existing.nombre = data['nombre']
                if data.get('ean'):
                    existing.ean = data['ean']
                db.add(Precio(
                    producto_id=existing.id,
                    precio=data['precio'],
                    precio_original=None,
                    en_stock=data['en_stock'],
                    fecha_captura=datetime.now(timezone.utc)
                ))
                actualizados += 1
            else:
                prod = Producto(
                    nombre=data['nombre'],
                    url=url,
                    farmacia=FARMACIA,
                    categoria='',
                    ean=data.get('ean') or ''
                )
                db.add(prod)
                db.flush()
                db.add(Precio(
                    producto_id=prod.id,
                    precio=data['precio'],
                    precio_original=None,
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
