import requests
import re
from bs4 import BeautifulSoup

urls = [
    'https://okfarma.es/solgar-vitamina-b12-1000-mcg-cianocobalamina-100-comprimidos',
    'https://okfarma.es/pack-innovage-lipo-reductor-celulitico-2-x-30-comprimidos'
]

for url in urls:
    print(f"--- URL {url} ---")
    r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    html = r.text
    
    soup = BeautifulSoup(html, 'html.parser')
    
    m_amount = soup.find('meta', property='product:price:amount')
    print("Meta product:price:amount:", m_amount.get('content') if m_amount else "None")
    
    current_span = soup.find('span', class_='current-price-value')
    print("span class=current-price-value:", current_span.get('content') if current_span else "None", current_span.text if current_span else "")
    
    # regex for product:price:amount
    m_regex = re.search(r'product:price:amount\"\s*content=\"([\d\.]+)\"', html)
    print("Regex product:price:amount:", m_regex.group(1) if m_regex else "None")
    
    # let's look for "15.89" or "15,89" in the HTML manually
    find_real = re.findall(r'15[\.,]89|33[\.,]01', html)
    print("Manual find of real price:", find_real[:5])
    
    print()
