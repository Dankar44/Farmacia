"""
FarmaSearch - Base Scraper with multi-level fallback.

Each pharmacy scraper inherits from BaseScraper and implements
the available levels (API, HTTP, Playwright).

Usage:
    scraper = AtidaScraper()
    result = scraper.run(level=None, limit=100)  # Auto fallback 1→2→3
    result = scraper.run(level=2, limit=50)       # Force level 2
"""

import sys
import os
import logging
import argparse
from datetime import datetime, timezone
from decimal import Decimal

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_models import get_engine, Producto, Precio, Base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text


class ScraperResult:
    """Result of a scraper execution."""
    def __init__(self):
        self.total = 0
        self.nuevos = 0
        self.actualizados = 0
        self.errores = 0
        self.level_used = None
        self.status = "pending"
        self.message = ""


class BaseScraper:
    """
    Base class for all pharmacy scrapers.
    Subclasses must set FARMACIA and LEVELS, and implement the scrape methods.
    """

    FARMACIA = ""
    BASE_URL = ""
    SITEMAP_URL = ""

    # Override in subclass: set available=True for implemented levels
    LEVELS = {
        1: {"name": "API", "available": False, "description": "API directa"},
        2: {"name": "HTTP", "available": False, "description": "HTTP inteligente"},
        3: {"name": "Playwright", "available": False, "description": "Navegador completo"},
    }

    def __init__(self):
        self.logger = logging.getLogger(f"scraper.{self.FARMACIA}")
        self.engine = get_engine()
        self.Session = sessionmaker(bind=self.engine)

    def get_db_session(self):
        return self.Session()

    def run(self, level=None, limit=0):
        """
        Execute scraping. If level=None, tries each available level in order (1→2→3).
        Returns ScraperResult.
        """
        result = ScraperResult()

        if level is not None:
            levels_to_try = [level]
        else:
            levels_to_try = [1, 2, 3]

        for lvl in levels_to_try:
            if not self.LEVELS.get(lvl, {}).get("available", False):
                self.logger.info(f"Nivel {lvl} ({self.LEVELS[lvl]['name']}) no disponible, saltando...")
                continue

            self.logger.info(f"{'='*60}")
            self.logger.info(f"Intentando Nivel {lvl}: {self.LEVELS[lvl]['name']} - {self.FARMACIA}")
            self.logger.info(f"{'='*60}")

            try:
                if lvl == 1:
                    result = self.scrape_api(limit)
                elif lvl == 2:
                    result = self.scrape_http(limit)
                elif lvl == 3:
                    result = self.scrape_playwright(limit)

                result.level_used = lvl
                result.status = "success"
                self.logger.info(f"Nivel {lvl} completado: {result.total} productos")
                return result

            except Exception as e:
                self.logger.error(f"Nivel {lvl} fallo: {e}")
                result.errores += 1
                result.message = f"Nivel {lvl} fallo: {str(e)[:200]}"
                continue

        result.status = "error"
        result.message = result.message or "Todos los niveles fallaron"
        self.logger.error(result.message)
        return result

    def scrape_api(self, limit=0):
        """Level 1: Direct API scraping. Override in subclass."""
        raise NotImplementedError(f"{self.FARMACIA} no tiene implementado el nivel 1 (API)")

    def scrape_http(self, limit=0):
        """Level 2: HTTP with rotating headers. Override in subclass."""
        raise NotImplementedError(f"{self.FARMACIA} no tiene implementado el nivel 2 (HTTP)")

    def scrape_playwright(self, limit=0):
        """Level 3: Full browser automation. Override in subclass."""
        raise NotImplementedError(f"{self.FARMACIA} no tiene implementado el nivel 3 (Playwright)")

    @staticmethod
    def _clean_raw_name(nombre):
        """Strip pharmacy prefixes and normalize spacing."""
        import re
        nombre = re.sub(
            r'^\s*(PromoFarma|Atida|Mifarma|DosFarma|Farmacia\s*Barata|OkFarma|Farmacoslada|Farmacias\s*Direct|Farmacias\s*Vazquez)\s*[-:|/·]\s*',
            '', nombre, flags=re.IGNORECASE
        )
        return re.sub(r'\s+', ' ', nombre).strip()

    def save_product(self, db, nombre, url, farmacia, categoria, ean, precio, precio_original, en_stock):
        """Save or update a product in the database."""
        nombre = self._clean_raw_name(nombre)
        try:
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
                return "updated"
            else:
                prod = Producto(
                    nombre=nombre, url=url, farmacia=farmacia,
                    categoria=categoria or "", ean=ean or ""
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
                return "new"
        except Exception as e:
            self.logger.debug(f"Error saving {url}: {e}")
            return "error"

    @staticmethod
    def parse_cli_args():
        """Parse standard CLI arguments."""
        parser = argparse.ArgumentParser()
        parser.add_argument('--limit', type=int, default=0, help='Max products (0=all)')
        parser.add_argument('--level', type=int, default=None, help='Force scraping level (1/2/3)')
        parser.add_argument('--export', action='store_true', help='Export to Excel only')
        parser.add_argument('--output', type=str, help='Custom output path')
        return parser.parse_args()
