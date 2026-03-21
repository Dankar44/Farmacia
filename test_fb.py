import requests
import re
from bs4 import BeautifulSoup

headers = {'User-Agent': 'Mozilla/5.0'}
url = 'https://www.farmaciabarata.es/module/lgsitemaps/sitemap?fc=module&name=product_1_1'

r = requests.get(url, headers=headers)
urls = re.findall(r'<loc>(https://www.farmaciabarata.es/[^<]+)</loc>', r.text)

if urls:
    product_url = urls[2] # Let's try the 3rd product
    print(f"Testing URL: {product_url}")
    p_r = requests.get(product_url, headers=headers)
    html = p_r.text
    
    soup = BeautifulSoup(html, 'html.parser')
    
    # Try to find reference wrapper
    refs = soup.find_all(lambda tag: tag.name and 'ref' in tag.name.lower() or tag.get('class') and any('ref' in c.lower() for c in tag.get('class')))
    for r in refs[:5]:
        print("REF Node:", r.get_text(strip=True)[:100])
        
    eans = soup.find_all(lambda tag: tag.name and 'ean' in tag.name.lower() or tag.get('class') and any('ean' in c.lower() for c in tag.get('class')))
    for e in eans[:5]:
        print("EAN Node:", e.get_text(strip=True)[:100])
        
    # Also just regex search
    import json
    # Many prestashops have a var prestashop
    match = re.search(r'var prestashop = (\{.*?\});', html, re.DOTALL)
    if match:
        try:
            data = json.loads(match.group(1))
            print("Prestashop EAN13:", data.get('product', {}).get('ean13'))
            print("Prestashop Reference:", data.get('product', {}).get('reference'))
        except Exception as e:
            print("Error parsing json:", e)
    else:
        # Just generic search
        print("Generic EAN match:", re.findall(r'ean.*?(\d{6,13})', html, re.I)[:5])
        print("Generic Ref match:", re.findall(r'ref.*?(\d{6,13})', html, re.I)[:5])
