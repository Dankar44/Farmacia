"""
FarmaSearch - HTTP Utilities for Level 2 scraping.
Shared functions for intelligent HTTP scraping with rotating headers,
retries with exponential backoff, sitemap parsing, and HTML extraction.
"""

import random
import time
import logging
import re
import xml.etree.ElementTree as ET
from decimal import Decimal, InvalidOperation

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Pool of real browser User-Agents (updated 2026)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.5; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 OPR/116.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Vivaldi/7.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Brave/1.72",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Vivaldi/7.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
]

ACCEPT_LANGUAGES = [
    "es-ES,es;q=0.9,en;q=0.8",
    "es-ES,es;q=0.9",
    "es,en;q=0.9,fr;q=0.8",
    "es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3",
]


def get_random_headers(referer=None):
    """Generate realistic random browser headers."""
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": random.choice(ACCEPT_LANGUAGES),
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "max-age=0",
    }
    if referer:
        headers["Referer"] = referer
    return headers


def fetch_with_retry(url, max_retries=3, base_delay=1.0, referer=None, timeout=30):
    """Fetch URL with exponential backoff retry and rotating headers."""
    for attempt in range(max_retries):
        try:
            headers = get_random_headers(referer=referer)
            response = requests.get(url, headers=headers, timeout=timeout)

            if response.status_code == 200:
                return response

            if response.status_code == 429:  # Rate limited
                wait = base_delay * (2 ** attempt) + random.uniform(0, 1)
                logger.warning(f"Rate limited (429) on {url}. Waiting {wait:.1f}s...")
                time.sleep(wait)
                continue

            if response.status_code in (403, 503):  # Blocked or service unavailable
                wait = base_delay * (2 ** attempt) + random.uniform(0, 2)
                logger.warning(f"HTTP {response.status_code} on {url}. Retry {attempt+1}/{max_retries} in {wait:.1f}s")
                time.sleep(wait)
                continue

            if response.status_code == 404:
                return None  # Not found, don't retry

            logger.warning(f"HTTP {response.status_code} on {url}")
            return None

        except requests.exceptions.Timeout:
            wait = base_delay * (2 ** attempt)
            logger.warning(f"Timeout on {url}. Retry {attempt+1}/{max_retries} in {wait:.1f}s")
            time.sleep(wait)
        except requests.exceptions.ConnectionError as e:
            wait = base_delay * (2 ** attempt)
            logger.warning(f"Connection error on {url}: {e}. Retry {attempt+1}/{max_retries}")
            time.sleep(wait)
        except Exception as e:
            logger.error(f"Unexpected error fetching {url}: {e}")
            return None

    logger.error(f"All {max_retries} retries failed for {url}")
    return None


def download_sitemap(sitemap_url, filter_pattern=None):
    """Download and parse XML sitemap, returning list of product URLs."""
    logger.info(f"Downloading sitemap: {sitemap_url}")
    resp = fetch_with_retry(sitemap_url, max_retries=3, timeout=60)
    if not resp:
        logger.error(f"Failed to download sitemap: {sitemap_url}")
        return []

    urls = []
    try:
        # Handle encoding issues
        content = resp.content
        if content[:3] == b'\xef\xbb\xbf':  # BOM
            content = content[3:]
        root = ET.fromstring(content)
        ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

        # Check if it's a sitemap index
        sitemaps = root.findall('.//ns:sitemap/ns:loc', ns)
        if sitemaps:
            logger.info(f"Sitemap index found with {len(sitemaps)} sub-sitemaps")
            for sm in sitemaps:
                sm_url = sm.text.strip()
                if filter_pattern and not re.search(filter_pattern, sm_url):
                    continue
                sub_urls = download_sitemap(sm_url)
                urls.extend(sub_urls)
                time.sleep(random.uniform(0.3, 0.8))
        else:
            # Direct sitemap with URLs
            locs = root.findall('.//ns:url/ns:loc', ns)
            for loc in locs:
                url = loc.text.strip()
                if filter_pattern and not re.search(filter_pattern, url):
                    continue
                urls.append(url)

    except ET.ParseError as e:
        logger.error(f"XML parse error for sitemap {sitemap_url}: {e}")
    except Exception as e:
        logger.error(f"Error processing sitemap {sitemap_url}: {e}")

    logger.info(f"Found {len(urls)} URLs from sitemap")
    return urls


def extract_price(text):
    """Extract decimal price from text string."""
    if not text:
        return None
    try:
        # Remove currency symbols, spaces, and normalize
        cleaned = re.sub(r'[€$\s]', '', str(text).strip())
        cleaned = cleaned.replace(',', '.')
        # Find the number
        match = re.search(r'(\d+\.?\d*)', cleaned)
        if match:
            return Decimal(match.group(1)).quantize(Decimal('0.01'))
    except (InvalidOperation, ValueError):
        pass
    return None


def extract_product_from_html(html_content, base_url, config):
    """
    Extract product data from HTML using CSS selectors.

    config = {
        'name_selector': 'h1',
        'price_selector': 'meta[property="product:price:amount"]',
        'price_attr': 'content',  # or None for text
        'stock_selector': '.add-to-cart',  # if exists = in stock
        'ean_selector': '[itemprop="sku"]',
        'ean_attr': 'content',
    }
    """
    soup = BeautifulSoup(html_content, 'lxml')

    # Name
    name_el = soup.select_one(config.get('name_selector', 'h1'))
    name = name_el.get_text(strip=True) if name_el else None
    if not name:
        title = soup.find('title')
        name = title.get_text(strip=True) if title else None

    # Price
    price = None
    price_sel = config.get('price_selector')
    if price_sel:
        price_el = soup.select_one(price_sel)
        if price_el:
            price_attr = config.get('price_attr')
            raw = price_el.get(price_attr) if price_attr else price_el.get_text(strip=True)
            price = extract_price(raw)

    # Stock
    in_stock = True
    stock_sel = config.get('stock_selector')
    if stock_sel:
        stock_el = soup.select_one(stock_sel)
        if config.get('stock_inverted', False):
            in_stock = stock_el is None  # Element present means OUT of stock
        else:
            in_stock = stock_el is not None  # Element present means IN stock

    # Only check for out-of-stock in product-specific areas, not the whole page
    product_area = soup.select_one('.product-info, .product-container, [itemprop="offers"], .product-prices')
    if product_area:
        area_text = product_area.get_text().lower()
        if any(x in area_text for x in ['agotado', 'fuera de stock', 'sin stock']):
            in_stock = False

    # EAN
    ean = None
    ean_sel = config.get('ean_selector')
    if ean_sel:
        ean_el = soup.select_one(ean_sel)
        if ean_el:
            ean_attr = config.get('ean_attr')
            ean = ean_el.get(ean_attr) if ean_attr else ean_el.get_text(strip=True)

    return {
        'nombre': name,
        'precio': price,
        'en_stock': in_stock,
        'ean': ean,
    }


def random_delay(min_s=0.5, max_s=2.0):
    """Sleep for a random duration between min and max seconds."""
    time.sleep(random.uniform(min_s, max_s))
