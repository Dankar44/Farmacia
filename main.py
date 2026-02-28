"""
Farmacia Price Scraper - Punto de entrada principal.

Uso:
    python main.py dosfarma                    # Scrapear DosFarma (todos los productos)
    python main.py dosfarma --limit 100        # Solo 100 productos (test)
    python main.py dosfarma --export           # Solo exportar datos a Excel
    python main.py dosfarma --refresh-key      # Renovar API Key + scrapear

En el futuro:
    python main.py promofarma                  # Scrapear PromoFarma
    python main.py todas                       # Scrapear todas las farmacias
"""

import sys

FARMACIAS_DISPONIBLES = {
    "dosfarma": "scrapers.dosfarma",
    "farmaciasdirect": "scrapers.farmaciasdirect",
    "promofarma": "scrapers.promofarma",
    "atida": "scrapers.atida",
    "vazquez": "scrapers.vazquez",
}


def mostrar_ayuda():
    print("=" * 50)
    print("  FARMACIA PRICE SCRAPER")
    print("=" * 50)
    print()
    print("Uso: python main.py <farmacia> [opciones]")
    print()
    print("Farmacias disponibles:")
    for nombre in FARMACIAS_DISPONIBLES:
        print(f"  - {nombre}")
    print()
    print("Opciones:")
    print("  --limit N       Limitar a N productos")
    print("  --export        Solo exportar a Excel")
    print("  --refresh-key   Renovar API Key (si da error 403)")
    print("  --output FILE   Ruta del archivo de salida")
    print()
    print("Ejemplos:")
    print("  python main.py dosfarma")
    print("  python main.py dosfarma --limit 50")
    print("  python main.py dosfarma --export")
    print()


def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        mostrar_ayuda()
        return
    
    farmacia = sys.argv[1].lower()
    
    if farmacia == "todas":
        # Ejecutar todos los scrapers
        for nombre, modulo in FARMACIAS_DISPONIBLES.items():
            print(f"\n{'=' * 50}")
            print(f"  Scrapeando: {nombre.upper()}")
            print(f"{'=' * 50}")
            mod = __import__(modulo, fromlist=["main"])
            # Quitar el nombre de farmacia de los argumentos
            sys.argv = [sys.argv[0]] + sys.argv[2:]
            mod.main()
        return
    
    if farmacia not in FARMACIAS_DISPONIBLES:
        print(f"Error: Farmacia '{farmacia}' no encontrada.")
        print(f"Disponibles: {', '.join(FARMACIAS_DISPONIBLES.keys())}")
        return
    
    # Importar y ejecutar el scraper de la farmacia seleccionada
    modulo = FARMACIAS_DISPONIBLES[farmacia]
    mod = __import__(modulo, fromlist=["main"])
    
    # Quitar el nombre de farmacia de sys.argv para que argparse funcione
    sys.argv = [sys.argv[0]] + sys.argv[2:]
    mod.main()


if __name__ == "__main__":
    main()
