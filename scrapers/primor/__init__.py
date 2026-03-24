"""Primor scraper - Level 2 (HTTP) and Level 3 (Playwright)."""


def main():
    """Default entry point: run Level 2 HTTP scraper."""
    from scrapers.primor.level2_http import main as l2_main
    l2_main()
