import sys
import json
import requests
from bs4 import BeautifulSoup
import time

def check_pharmacy(name, url):
    print(f"\n======================================")
    print(f"Analizando: {name} ({url})")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
    }
    
    try:
        # Check basic accessibility
        resp = requests.get(url, headers=headers, timeout=10)
        print(f"Estado HTTP: {resp.status_code}")
        
        # Check sitemap
        sitemap_url = ""
        if url.endswith('/'):
            sitemap_url = url + "sitemap.xml"
        else:
            sitemap_url = url + "/sitemap.xml"
            
        print(f"Buscando sitemap en: {sitemap_url}")
        s_resp = requests.get(sitemap_url, headers=headers, timeout=10)
        if s_resp.status_code == 200:
            print(f"  [+] Sitemap encontrado (Longitud: {len(s_resp.text)} bytes)")
            if 'es_1_sitemap.xml' in s_resp.text or 'sitemap_products' in s_resp.text:
                print("  [+] Parece tener sitemaps dedicados de productos.")
        else:
            print(f"  [-] Sitemap estándar no encontrado (HTTP {s_resp.status_code})")
            
        # Analyze technology stack
        soup = BeautifulSoup(resp.text, 'html.parser')
        html_content = resp.text.lower()
        
        print(f"Tecnologías detectadas:")
        
        # Check for PrestaShop
        if 'prestashop' in html_content or 'modules/ps_' in html_content:
            print("  [+] Backend probable: PRESTASHOP")
        # Check for Magento
        elif 'magento' in html_content:
            print("  [+] Backend probable: MAGENTO")
        # Check for Vtex
        elif 'vtex' in html_content:
            print("  [+] Backend probable: VTEX")
        # Check for Shopify
        elif 'shopify' in html_content:
            print("  [+] Backend probable: SHOPIFY")
            
        # Check primary search engine
        if 'doofinder' in html_content:
            print("  [+] Buscador: DOOFINDER (API accesible)")
        elif 'algolia' in html_content:
            print("  [+] Buscador: ALGOLIA (API accesible)")
        elif 'empathy' in html_content:
            print("  [+] Buscador: EMPATHY.CO (API accesible)")
            
        # Look for API endpoints in the page source
        api_hints = [alg for alg in ['algolia', 'doofinder', 'empathy', 'api_key', 'hashid'] if alg in html_content]
        if api_hints:
            print(f"  [!] Pistas de API encontradas en el HTML: {api_hints}")
            
    except Exception as e:
        print(f"Error analizando {name}: {e}")
        
    time.sleep(1)

pharmacies = [
    ("Farma2Go", "https://www.farma2go.com/"),
    ("Farmacia Barata", "https://www.farmaciabarata.es/"),
    ("OKfarma", "https://okfarma.es/"),
    ("Farmacia Morlan", "https://www.farmaciamorlan.com/"),
    ("Farmacias Trebol", "https://farmaciastrebol.com/")
]

for name, url in pharmacies:
    check_pharmacy(name, url)
