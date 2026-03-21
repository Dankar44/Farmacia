"""
Level 2 HTTP scraper for PromoFarma (promofarma.com)
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

FARMACIA = "PromoFarma"
BASE_URL = "https://www.promofarma.com"
SITEMAP_URL = "https://www.promofarma.com/es/sitemaps/index.xml"
BATCH_SIZE = 100

HTML_CONFIG = {
    'name_selector': '[data-product-name], h1',
    'price_selector': 'meta[property="product:price:amount"]',
    'price_attr': 'content',
    'stock_selector': None,  # Handled by custom logic
    'ean_selector': '[data-product-id]',
    'ean_attr': 'data-product-id',
}

logger = logging.getLogger(FARMACIA)
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(name)s] %(message)s')


def is_product_url(url):
    """Filter for product URLs. PromoFarma products often match /p-{id} pattern."""
    # PromoFarma product URLs contain /es/ and often /p- pattern
    if '/es/' not in url:
        return False
    skip_patterns = [
        '/categoria/', '/categorias/', '/marca/', '/blog/',
        '/info/', '/login', '/registro', '/checkout',
        '/carrito', '/contacto', '/ayuda/', '/faq/',
        '/politica', '/condiciones', '/sitemaps/',
    ]
    for pattern in skip_patterns:
        if pattern in url.lower():
            return False
    if url.rstrip('/') == BASE_URL:
        return False
    return True


def extract_name(html_text):
    """Extract product name with PromoFarma-specific logic."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html_text, 'lxml')

    # Try data-product-name attribute
    el = soup.select_one('[data-product-name]')
    if el:
        name = el.get('data-product-name')
        if name:
            return name.strip()

    # Fallback to h1
    h1 = soup.find('h1')
    if h1:
        return h1.get_text(strip=True)

    # Fallback to title
    title = soup.find('title')
    if title:
        return title.get_text(strip=True).split('|')[0].strip()

    return None


def extract_price_custom(html_text):
    """Extract price with PromoFarma-specific logic."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html_text, 'lxml')

    # Try data-pvp attribute
    el = soup.select_one('[data-pvp]')
    if el:
        price = extract_price(el.get('data-pvp'))
        if price:
            return price

    # Try meta product:price:amount
    meta = soup.select_one('meta[property="product:price:amount"]')
    if meta:
        price = extract_price(meta.get('content'))
        if price:
            return price

    # Try JSON-LD
    for script in soup.find_all('script', type='application/ld+json'):
        text = script.get_text()
        price_match = re.search(r'"price"\s*:\s*["\']?([\d.,]+)', text)
        if price_match:
            price = extract_price(price_match.group(1))
            if price:
                return price

    return None


def extract_ean_custom(html_text, url):
    """Extract EAN/product ID from PromoFarma page."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html_text, 'lxml')

    # Try data-product-id attribute
    el = soup.select_one('[data-product-id]')
    if el:
        val = el.get('data-product-id')
        if val:
            return val

    # Try URL pattern /p-{id}
    match = re.search(r'/p-(\d+)', url)
    if match:
        return match.group(1)

    # Try JSON-LD sku
    for script in soup.find_all('script', type='application/ld+json'):
        text = script.get_text()
        sku_match = re.search(r'"sku"\s*:\s*"([^"]+)"', text)
        if sku_match:
            return sku_match.group(1)

    return None


def check_stock(html_text):
    """Check stock by looking for InStock or out-of-stock markers."""
    text_lower = html_text.lower()

    # Check for out-of-stock markers
    out_of_stock_markers = ['agotado', 'fuera de stock', 'sin stock', 'no disponible', 'out of stock']
    for marker in out_of_stock_markers:
        if marker in text_lower:
            return False

    # Check for InStock in schema markup
    if 'instock' in text_lower:
        return True
    if 'outofstock' in text_lower:
        return False

    return True  # Default to in stock


def scrape(limit=0):
    """Run the Level 2 HTTP scraper for PromoFarma."""
    engine = get_engine()
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    db = Session()

    logger.info(f"Descargando sitemap index: {SITEMAP_URL}")
    # PromoFarma uses a sitemap index; filter for product sitemaps
    urls = download_sitemap(SITEMAP_URL, filter_pattern=r'product|producto')
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

            # Custom extraction for PromoFarma
            nombre = extract_name(resp.text)
            precio = extract_price_custom(resp.text)
            ean = extract_ean_custom(resp.text, url)
            en_stock = check_stock(resp.text)

            if not nombre or not precio:
                errors += 1
                continue

            existing = db.query(Producto).filter_by(url=url).first()
            if existing:
                existing.nombre = nombre
                if ean:
                    existing.ean = ean
                db.add(Precio(
                    producto_id=existing.id,
                    precio=precio,
                    precio_original=None,
                    en_stock=en_stock,
                    fecha_captura=datetime.now(timezone.utc)
                ))
                actualizados += 1
            else:
                prod = Producto(
                    nombre=nombre,
                    url=url,
                    farmacia=FARMACIA,
                    categoria='',
                    ean=ean or ''
                )
                db.add(prod)
                db.flush()
                db.add(Precio(
                    producto_id=prod.id,
                    precio=precio,
                    precio_original=None,
                    en_stock=en_stock,
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
