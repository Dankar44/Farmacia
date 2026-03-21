import json
import re
from bs4 import BeautifulSoup as bs

html = open('okfarma_sample.html', encoding='utf-8').read()
soup = bs(html, 'html.parser')

print("=== EXTRACCION DE DATOS OKFARMA ===")
# 1. Nombre
h1 = soup.find('h1')
nombre = h1.text.strip() if h1 else 'No encontrado'
print(f"Nombre: {nombre}")

# 2. Precio
precio = 'No encontrado'
price_span = soup.find('span', class_='current-price-value')
if price_span and price_span.has_attr('content'):
    precio = price_span['content']
else:
    price_span = soup.find('span', class_='price_pvp')
    if price_span and price_span.has_attr('content'):
        precio = price_span['content']
print(f"Precio: {precio}")

# 3. EAN
ean = 'No encontrado'
# Buscamos en el meta tag o script
script_tags = soup.find_all('script')
for script in script_tags:
    if script.string and 'ean13' in script.string:
        match = re.search(r'"ean13"\s*:\s*"(\d{13})"', script.string)
        if match:
            ean = match.group(1)
            break
if ean == 'No encontrado':
    match = re.search(r'ean13\\*\"*\s*:\s*\\*\"*(\d{13})', html)
    if match: ean = match.group(1)
print(f"EAN: {ean}")

# 4. En Stock
en_stock = False
if 'Agotado' not in html and 'Fuera de stock' not in html:
    en_stock = True
print(f"En stock: {en_stock}")
