import requests
from bs4 import BeautifulSoup
import re
import xml.etree.ElementTree as ET

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

def get_doofinder_hash(url):
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        # Buscar hashid en el HTML (suelen estar en el script de configuración)
        # "hashid": "..."
        matches = re.findall(r'hashid[\'"]?\s*[:=]\s*[\'"]([a-zA-Z0-9]{32})[\'"]', resp.text, re.IGNORECASE)
        if not matches:
            # Otra variante común de doofinder
            matches = re.findall(r'doofinder-.*?-([a-zA-Z0-9]{32})', resp.text, re.IGNORECASE)
        
        if matches:
            return matches[0]
            
        # Buscar scripts .js externos que puedan contener 'doofinder'
        soup = BeautifulSoup(resp.text, 'html.parser')
        scripts = soup.find_all('script', src=True)
        for s in scripts:
            if 'doofinder' in s['src']:
                # a veces el script es tipo dlid=blabla
                id_match = re.search(r'dlid=([a-zA-Z0-9]{32})', s['src'])
                if id_match:
                    return id_match.group(1)
                
                # Fetch el script
                s_url = s['src'] if s['src'].startswith('http') else url.rstrip('/') + s['src']
                try:
                    s_resp = requests.get(s_url, headers=headers, timeout=10)
                    m2 = re.findall(r'hashid[\'"]?\s*[:=]\s*[\'"]([a-zA-Z0-9]{32})[\'"]', s_resp.text, re.IGNORECASE)
                    if m2:
                        return m2[0]
                except:
                    pass
    except Exception as e:
        print(f"Error extrayendo hashid de {url}: {e}")
    return None

def count_doofinder(hashid):
    if not hashid:
        return "No se pudo obtener HashID"
    url = "https://eu1-search.doofinder.com/5/search"
    params = {
        "hashid": hashid,
        "query": "",
        "page": 1,
        "rpp": 1
    }
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=10)
        data = resp.json()
        return data.get("total", "Desconocido")
    except:
        return "Error en API"

def count_sitemap(sitemap_url):
    try:
        resp = requests.get(sitemap_url, headers=headers, timeout=10)
        text = resp.text
        
        root = ET.fromstring(resp.content)
        # Check if it's a sitemap index
        total = 0
        sitemaps = root.findall('{http://www.sitemaps.org/schemas/sitemap/0.9}sitemap')
        if sitemaps:
            for s in sitemaps:
                loc = s.find('{http://www.sitemaps.org/schemas/sitemap/0.9}loc')
                if loc is not None and ('product' in loc.text or 'articulo' in loc.text or 'item' in loc.text):
                    try:
                        sub_resp = requests.get(loc.text, headers=headers, timeout=10)
                        sub_root = ET.fromstring(sub_resp.content)
                        urls = sub_root.findall('{http://www.sitemaps.org/schemas/sitemap/0.9}url')
                        total += len(urls)
                    except:
                        pass
        else:
            urls = root.findall('{http://www.sitemaps.org/schemas/sitemap/0.9}url')
            total = len(urls)
            
        if total == 0:
            # Alternativa: texto rudo (cuidado con otras urls)
            pass
            
        return total
    except Exception as e:
        return f"Error leyendo sitemap: {e}"

print("=== Farma2Go ===")
f2g_hash = "109033ba12920f3ea2fe8ea3f3b97b0a" # Found in their main JS usually, let's try auto first
h = get_doofinder_hash("https://www.farma2go.com/")
print(f"HashID: {h}")
print(f"Productos Doofinder: {count_doofinder(h)}")

print("\n=== Farmacia Morlan ===")
morlan_hash = get_doofinder_hash("https://www.farmaciamorlan.com/")
print(f"HashID: {morlan_hash}")
print(f"Productos Doofinder: {count_doofinder(morlan_hash)}")

print("\n=== Farmacia Barata ===")
print(f"Productos Sitemap: {count_sitemap('https://www.farmaciabarata.es/sitemap.xml')}")
