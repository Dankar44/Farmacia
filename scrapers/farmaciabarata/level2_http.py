"""
Level 2 HTTP scraper for FarmaciaBarata (farmaciabarata.es)
Sitemap-based scraping with rotating headers, retries, and BeautifulSoup parsing.
"""
import sys
import os
import re
import json
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

FARMACIA = "FarmaciaBarata"
BASE_URL = "https://farmaciabarata.es"
SITEMAP_URL = "https://www.farmaciabarata.es/module/lgsitemaps/sitemap?name=sitemap_1"
BATCH_SIZE = 100

HTML_CONFIG = {
    'name_selector': 'h1',
    'price_selector': 'meta[property="product:price:amount"]',
    'price_attr': 'content',
    'stock_selector': '[data-button-action="add-to-cart"]',
    'ean_selector': '[itemprop="sku"]',
    'ean_attr': 'content',
}

logger = logging.getLogger(FARMACIA)
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(name)s] %(message)s')


def is_product_url(url):
    """Filter for product URLs. FarmaciaBarata uses PrestaShop-style URLs."""
    # Must end with .html for product pages in PrestaShop
    if not url.endswith('.html'):
        return False
    skip_patterns = [
        '/categoria/', '/categorias/', '/content/',
        '/info/', '/login', '/registro', '/checkout',
        '/carrito', '/contacto', '/ayuda/', '/blog/',
        '/module/', '/modulo/', '/cms/',
    ]
    for pattern in skip_patterns:
        if pattern in url.lower():
            return False
    base = BASE_URL.replace('https://', '').replace('http://', '')
    url_clean = url.replace('https://', '').replace('http://', '').replace('www.', '')
    if url_clean.rstrip('/') == base.replace('www.', ''):
        return False
    return True


def extract_price_custom(html_text):
    """Extract price with FarmaciaBarata-specific logic."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html_text, 'lxml')

    # Try itemprop="price" content attribute
    el = soup.select_one('[itemprop="price"]')
    if el:
        price = extract_price(el.get('content') or el.get_text(strip=True))
        if price:
            return price

    # Try JSON in script tags (PrestaShop pattern)
    for script in soup.find_all('script'):
        text = script.get_text()
        # Look for product price in JS object
        match = re.search(r'"price"\s*:\s*["\']?([\d.,]+)', text)
        if match:
            price = extract_price(match.group(1))
            if price:
                return price
        match = re.search(r'"price_amount"\s*:\s*["\']?([\d.,]+)', text)
        if match:
            price = extract_price(match.group(1))
            if price:
                return price

    # Try meta product:price:amount
    meta = soup.select_one('meta[property="product:price:amount"]')
    if meta:
        price = extract_price(meta.get('content'))
        if price:
            return price

    # Try common price selectors
    for selector in ['.price', '.current-price', '.product-price', '.our_price_display']:
        el = soup.select_one(selector)
        if el:
            price = extract_price(el.get_text(strip=True))
            if price:
                return price

    return None


def extract_ean_custom(html_text):
    """Extract EAN from FarmaciaBarata page, trying JSON ean13 and itemprop."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html_text, 'lxml')

    # Try ean13 in script tags (PrestaShop stores this in JS)
    for script in soup.find_all('script'):
        text = script.get_text()
        match = re.search(r'"ean13"\s*:\s*"(\d{7,13})"', text)
        if match:
            return match.group(1)
        match = re.search(r'ean13["\']?\s*[:=]\s*["\'](\d{7,13})', text)
        if match:
            return match.group(1)

    # Try itemprop="sku"
    el = soup.select_one('[itemprop="sku"]')
    if el:
        val = el.get('content') or el.get_text(strip=True)
        if val:
            return val

    # Try itemprop="gtin13"
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
    """Check stock by looking for Agotado, out-of-stock class, or availability markers."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html_text, 'lxml')
    text_lower = html_text.lower()

    # Check for out-of-stock text markers
    out_of_stock_markers = ['agotado', 'fuera de stock', 'sin stock', 'no disponible']
    for marker in out_of_stock_markers:
        if marker in text_lower:
            return False

    # Check for out-of-stock CSS class (PrestaShop pattern)
    if soup.select_one('.out-of-stock, .product-unavailable, .out_of_stock'):
        return False

    # Check availability schema
    avail = soup.select_one('[itemprop="availability"]')
    if avail:
        href = avail.get('href', '') or avail.get('content', '')
        if 'OutOfStock' in href:
            return False
        if 'InStock' in href:
            return True

    # Check JSON-LD availability
    if '"outofstock"' in text_lower:
        return False
    if '"instock"' in text_lower:
        return True

    # Check for add-to-cart button as positive signal
    cart_btn = soup.select_one('[data-button-action="add-to-cart"], .add-to-cart, #add-to-cart-or-refresh')
    if cart_btn:
        if cart_btn.get('disabled') is not None:
            return False
        return True

    return True  # Default to in stock


def scrape(limit=0):
    """Run the Level 2 HTTP scraper for FarmaciaBarata."""
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

            # Custom price extraction with fallback
            if not data['precio']:
                data['precio'] = extract_price_custom(resp.text)

            # Custom EAN extraction
            if not data.get('ean'):
                data['ean'] = extract_ean_custom(resp.text)

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
