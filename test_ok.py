import requests
import re
from bs4 import BeautifulSoup

url = 'https://okfarma.es/heliocare-360-pediatrics-transparent-spray-spf50-200-ml'
r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
html = r.text

print("Testing Okfarma Extractors")

prices = re.findall(r'\"price[^\"]*\"\s*:\s*\"?([\d\.]+)\"?', html)
print("All \"price*\" json matches:", prices[:15])

# Prestashop usually uses:
# <span class="current-price-value" content="22.31">
match_span = re.search(r'class=\"current-price-value\"[^>]*content=\"([\d\.]+)\"', html)
print("current-price-value span:", match_span.group(1) if match_span else "Not found")

# <meta property="product:price:amount" content="22.31">
meta_price = re.search(r'product:price:amount\"\s*content=\"([\d\.]+)\"', html)
print("meta product:price:amount:", meta_price.group(1) if meta_price else "Not found")

# Reference extraction
sku_match = re.search(r'Referencia.*?(\d{5,8})', html, re.I | re.DOTALL)
print("Regex Ref:", sku_match.group(1) if sku_match else "None")
