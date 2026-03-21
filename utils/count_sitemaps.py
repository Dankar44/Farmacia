import requests
import xml.etree.ElementTree as ET
import re
import sys

headers = {'User-Agent': 'Mozilla/5.0'}

def count_products_in_sitemap(index_url, product_keywords=['product', 'articulo', 'item', 'sitemap-1-']):
    print(f"\nAnalizando sitemap: {index_url}")
    try:
        resp = requests.get(index_url, headers=headers, timeout=15)
        if resp.status_code != 200:
            print(f"Error HTTP {resp.status_code}")
            return 0
            
        root = ET.fromstring(resp.content)
        total_urls = 0
        sitemaps = root.findall('{http://www.sitemaps.org/schemas/sitemap/0.9}sitemap')
        
        if sitemaps:
            print(f"Es un índice con {len(sitemaps)} sub-sitemaps.")
            for s in sitemaps:
                loc = s.find('{http://www.sitemaps.org/schemas/sitemap/0.9}loc')
                if loc is not None and loc.text:
                    url = loc.text.lower()
                    if any(kw in url for kw in product_keywords) or 'blog' not in url and 'category' not in url and 'author' not in url:
                        print(f"  Descargando: {loc.text}")
                        try:
                            sub_resp = requests.get(loc.text, headers=headers, timeout=15)
                            sub_root = ET.fromstring(sub_resp.content)
                            urls = sub_root.findall('{http://www.sitemaps.org/schemas/sitemap/0.9}url')
                            count = len(urls)
                            print(f"    -> {count} productos")
                            total_urls += count
                        except Exception as e:
                            print(f"    -> Error: {e}")
        else:
            urls = root.findall('{http://www.sitemaps.org/schemas/sitemap/0.9}url')
            total_urls = len(urls)
            
        return total_urls
    except Exception as e:
        print(f"Error general: {e}")
        return 0

print("Farma2Go: ", count_products_in_sitemap("https://www.farma2go.com/sitemap.xml", ['product', 'es_1_sitemap']))
print("Farmacia Morlan: ", count_products_in_sitemap("https://www.farmaciamorlan.com/sitemap.xml", ['product', '-1-', '-2-', '-3-']))
print("Farmacia Barata: ", count_products_in_sitemap("https://www.farmaciabarata.es/sitemap.xml", ['product']))
