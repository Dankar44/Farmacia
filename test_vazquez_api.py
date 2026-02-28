import sys
import os

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

from scrapers.vazquez import consultar_doofinder
import json

def test():
    query = "EXCILOR ESMALTE XL 7 ML"
    print(f"Buscando en Doofinder (Vazquez): {query}")
    data = consultar_doofinder(query, page=1)
    if data and "results" in data and len(data["results"]) > 0:
        hit = data["results"][0]
        print("\nRAW HIT de la API (Doofinder):")
        print(json.dumps(hit, indent=2, ensure_ascii=False))
        
        print("\nExtracción de precio actual en scraper:")
        p_val = hit.get("price")
        p_orig = hit.get("best_price") or hit.get("sale_price")
        print(f"price: {p_val}")
        print(f"best_price / sale_price: {p_orig}")
    else:
        print("No se encontró el producto.")

if __name__ == "__main__":
    test()
