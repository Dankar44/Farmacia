"""
Level 3 Playwright scraper for Primor (primor.eu)
Full browser automation for when HTTP scraping is blocked.
"""
import sys
import os
import re
import json
import logging
import argparse
import time
from datetime import datetime, timezone
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from db_models import get_engine, Producto, Precio, Base
from sqlalchemy.orm import sessionmaker
from scrapers.http_utils import extract_price, download_sitemap

FARMACIA = "Primor"
BASE_URL = "https://www.primor.eu"
SITEMAP_URL = "https://www.primor.eu/pub/media/sitemap_es_product.xml"
BATCH_SIZE = 100

logger = logging.getLogger(FARMACIA)
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(name)s] %(message)s')

# JavaScript to extract product data from a Primor page (runs in browser context)
EXTRACT_JS = """
(urls) => {
    return Promise.all(urls.map(url =>
        fetch(url, {credentials: 'include'})
            .then(r => r.text())
            .then(html => {
                const doc = new DOMParser().parseFromString(html, 'text/html');
                let nombre = null, precio = null, precioOriginal = null, ean = null, enStock = true, categoria = '';

                // JSON-LD
                const ldScripts = doc.querySelectorAll('script[type="application/ld+json"]');
                for (const s of ldScripts) {
                    try {
                        let d = JSON.parse(s.textContent);
                        if (Array.isArray(d)) d = d.find(i => i['@type'] === 'Product');
                        if (d && d['@type'] === 'Product') {
                            nombre = d.name;
                            ean = d.mpn || d.gtin13 || d.sku || null;
                            const offers = Array.isArray(d.offers) ? d.offers[0] : (d.offers || {});
                            if (offers.price) precio = parseFloat(offers.price);
                            if (offers.highPrice) precioOriginal = parseFloat(offers.highPrice);
                            if (offers.availability && offers.availability.includes('OutOfStock')) enStock = false;
                        }
                    } catch(e) {}
                }

                // Fallback: dataLayer
                if (!precio) {
                    const dlMatch = html.match(/"price"\\s*:\\s*([\\d.]+)/);
                    if (dlMatch) precio = parseFloat(dlMatch[1]);
                }

                // Fallback: HTML
                if (!nombre) {
                    const h1 = doc.querySelector('h1');
                    if (h1) nombre = h1.textContent.trim();
                }
                if (!precio) {
                    const meta = doc.querySelector('meta[property="product:price:amount"]');
                    if (meta) precio = parseFloat(meta.content);
                }

                // Category
                const crumbs = doc.querySelectorAll('.breadcrumbs li a span, .breadcrumb a');
                const parts = [];
                crumbs.forEach(c => {
                    const t = c.textContent.trim();
                    if (t && t.toLowerCase() !== 'inicio' && t.toLowerCase() !== 'home') parts.push(t);
                });
                if (parts.length) categoria = parts.slice(0, 3).join(' > ');

                return {url, nombre, precio, precioOriginal, ean, enStock, categoria};
            })
            .catch(() => ({url, nombre:null, precio:null, precioOriginal:null, ean:null, enStock:true, categoria:''}))
    ));
}
"""


def is_product_url(url):
    """Filter for Primor product URLs."""
    if '/es_es/' not in url:
        return False
    if not url.endswith('.html'):
        return False
    skip = ['/categoria/', '/customer/', '/checkout/', '/cart/', '/blog/', '/cms/', '/tiendas/', '/landing/']
    return not any(p in url.lower() for p in skip)


def scrape(limit=0):
    """Run the Level 3 Playwright scraper for Primor."""
    from playwright.sync_api import sync_playwright

    engine = get_engine()
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    db = Session()

    # Download sitemap via HTTP first (no anti-bot on sitemaps)
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

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            locale="es-ES"
        )
        page = context.new_page()

        # Navigate to base URL first to establish cookies/session
        logger.info("Estableciendo sesión del navegador...")
        page.goto(BASE_URL + "/es_es/", wait_until="domcontentloaded", timeout=30000)
        time.sleep(3)

        # Accept cookies if popup appears
        try:
            cookie_btn = page.query_selector('button#onetrust-accept-btn-handler, .cookie-accept, [data-action="accept-cookies"]')
            if cookie_btn:
                cookie_btn.click()
                time.sleep(1)
        except Exception:
            pass

        # Process in batches of 5
        batch_size = 5
        for batch_start in range(0, len(urls), batch_size):
            batch = urls[batch_start:batch_start + batch_size]

            try:
                results = page.evaluate(EXTRACT_JS, batch)
            except Exception as e:
                logger.error(f"Error en batch {batch_start}: {e}")
                errors += len(batch)
                continue

            for prod_data in results:
                try:
                    url = prod_data.get('url')
                    nombre = prod_data.get('nombre')
                    precio_raw = prod_data.get('precio')
                    precio_orig_raw = prod_data.get('precioOriginal')
                    ean = prod_data.get('ean')
                    en_stock = prod_data.get('enStock', True)
                    categoria = prod_data.get('categoria', '')

                    if not nombre or not precio_raw:
                        errors += 1
                        continue

                    precio = Decimal(str(precio_raw)).quantize(Decimal('0.01'))
                    precio_original = None
                    if precio_orig_raw:
                        precio_original = Decimal(str(precio_orig_raw)).quantize(Decimal('0.01'))

                    existing = db.query(Producto).filter_by(url=url).first()
                    if existing:
                        existing.nombre = nombre
                        if ean:
                            existing.ean = ean
                        if categoria:
                            existing.categoria = categoria
                        db.add(Precio(
                            producto_id=existing.id,
                            precio=precio,
                            precio_original=precio_original,
                            en_stock=en_stock,
                            fecha_captura=datetime.now(timezone.utc)
                        ))
                        actualizados += 1
                    else:
                        prod = Producto(
                            nombre=nombre, url=url, farmacia=FARMACIA,
                            categoria=categoria or '', ean=ean or ''
                        )
                        db.add(prod)
                        db.flush()
                        db.add(Precio(
                            producto_id=prod.id,
                            precio=precio,
                            precio_original=precio_original,
                            en_stock=en_stock,
                            fecha_captura=datetime.now(timezone.utc)
                        ))
                        nuevos += 1

                    saved += 1

                except Exception as e:
                    logger.error(f"Error guardando producto: {e}")
                    errors += 1
                    try:
                        db.rollback()
                    except Exception:
                        pass

            if saved % BATCH_SIZE == 0 and saved > 0:
                db.commit()

            processed = batch_start + len(batch)
            if processed % 50 == 0 or processed == len(urls):
                logger.info(f"Progreso: {processed}/{len(urls)} procesados | {saved} guardados | {errors} errores")

            time.sleep(0.8)

        browser.close()

    db.commit()
    db.close()
    logger.info(f"Completado {FARMACIA}: {saved} guardados ({nuevos} nuevos, {actualizados} actualizados), {errors} errores")
    return saved


def main():
    parser = argparse.ArgumentParser(description=f"Level 3 Playwright Scraper - {FARMACIA}")
    parser.add_argument('--limit', type=int, default=0, help='Max products to scrape (0=all)')
    parser.add_argument('--level', type=int, default=3, help='Scraping level (default=3)')
    args = parser.parse_args()
    scrape(limit=args.limit)


if __name__ == '__main__':
    main()
