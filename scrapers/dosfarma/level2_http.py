"""
Level 2 HTTP scraper for DosFarma (dosfarma.com)
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

FARMACIA = "DosFarma"
BASE_URL = "https://www.dosfarma.com"
SITEMAP_URL = "https://www.dosfarma.com/sitemap.xml"
BATCH_SIZE = 100

HTML_CONFIG = {
    'name_selector': 'h1',
    'price_selector': 'meta[property="product:price:amount"]',
    'price_attr': 'content',
    'stock_selector': '.add-to-cart, button[id*="add-to-cart"], #add-to-cart-button',
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
        '/carrito', '/contacto', '/ayuda/', '/faq',
        '/politica', '/aviso-legal', '/condiciones',
    ]
    for pattern in skip_patterns:
        if pattern in url.lower():
            return False
    # DosFarma product URLs typically end with .html or have a product slug
    if url.rstrip('/') == BASE_URL:
        return False
    return True


def extract_price_fallback(html_text):
    """Fallback price extraction from .price class or itemprop."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html_text, 'lxml')

    # Try .price class
    price_el = soup.select_one('.price, .product-price, .current-price')
    if price_el:
        price = extract_price(price_el.get_text(strip=True))
        if price:
            return price

    # Try itemprop="price"
    price_el = soup.select_one('[itemprop="price"]')
    if price_el:
        price = extract_price(price_el.get('content') or price_el.get_text(strip=True))
        if price:
            return price

    return None


def extract_ean_fallback(html_text):
    """Fallback EAN extraction from structured data."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html_text, 'lxml')

    # Try itemprop="sku" text content
    el = soup.select_one('[itemprop="sku"]')
    if el:
        val = el.get('content') or el.get_text(strip=True)
        if val:
            return val

    # Try JSON-LD
    for script in soup.find_all('script', type='application/ld+json'):
        text = script.get_text()
        sku_match = re.search(r'"sku"\s*:\s*"([^"]+)"', text)
        if sku_match:
            return sku_match.group(1)
        gtin_match = re.search(r'"gtin13"\s*:\s*"(\d{13})"', text)
        if gtin_match:
            return gtin_match.group(1)

    return None


def check_stock(html_text):
    """Check stock by looking for add-to-cart button and out-of-stock markers."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html_text, 'lxml')

    # Check for out-of-stock text
    text_lower = html_text.lower()
    out_of_stock_markers = ['agotado', 'fuera de stock', 'sin stock', 'no disponible']
    for marker in out_of_stock_markers:
        if marker in text_lower:
            return False

    # Check for add-to-cart button presence
    cart_btn = soup.select_one('.add-to-cart, button[id*="add-to-cart"], #add-to-cart-button, [data-button-action="add-to-cart"]')
    if cart_btn:
        if 'disabled' in cart_btn.attrs or cart_btn.get('disabled'):
            return False
        return True

    # Check schema availability
    if '"instock"' in text_lower:
        return True

    return True  # Default to in stock


def scrape(limit=0):
    """Run the Level 2 HTTP scraper for DosFarma."""
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
