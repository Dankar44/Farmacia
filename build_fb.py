import re
with open('scrapers/okfarma.py', 'r', encoding='utf-8') as f:
    code = f.read()

# Replace general names
code = code.replace('Okfarma', 'FarmaciaBarata')
code = code.replace('okfarma.es', 'farmaciabarata.es')
code = code.replace('okfarma', 'farmaciabarata')

# Fix sitemap index
code = code.replace('https://www.farmaciabarata.es/sitemap-1.xml', 'https://www.farmaciabarata.es/module/lgsitemaps/sitemap?name=sitemap_1')
code = code.replace('sitemap-products-', 'product_')

# Replace EXTRACT_FROM_HTML_JS completely
new_js = r'''
(urls) => {
    function decodeEntities(s) {
        if (!s) return s;
        return s.replace(/&#x([0-9a-f]+);/gi, (_, hex) => String.fromCharCode(parseInt(hex, 16)))
                .replace(/&#(\d+);/g, (_, dec) => String.fromCharCode(parseInt(dec)))
                .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
                .replace(/&quot;/g, '"').replace(/&apos;/g, "'");
    }

    return Promise.all(urls.map(url =>
        fetch(url)
            .then(r => r.text())
            .then(html => {
                const data = {url: url, ok: true};

                const titleMatch = html.match(/<h1[^>]*>([^<]+)<\/h1>/i);
                if (titleMatch) data.nombre = decodeEntities(titleMatch[1].trim());

                let precio = null;
                const priceMatch = html.match(/"price"\s*:\s*"?([\d.]+)"?/i);
                if (priceMatch) {
                    precio = parseFloat(priceMatch[1]);
                } else {
                    const priceFallback = html.match(/<span[^>]*itemprop="price"[^>]*content="([\d.]+)"/i);
                    if (priceFallback) precio = parseFloat(priceFallback[1]);
                }
                data.precio = precio;

                let sku = "";
                const eanMatch = html.match(/"ean13"\s*:\s*"(\d+)"/i);
                if (eanMatch) {
                    sku = eanMatch[1];
                } else {
                    const refMatch = html.match(/Referencia.*?(\d{5,13})/i);
                    if (refMatch) {
                        sku = refMatch[1].trim();
                    } else {
                        const skuMatch = html.match(/itemprop="sku"[^>]*>([^<]+)</i);
                        if (skuMatch) sku = skuMatch[1].trim();
                    }
                }
                data.sku = sku;

                data.enStock = !html.includes('id="out-of-stock"') && !html.includes('Agotado') && !html.includes('out-of-stock');

                return data;
            })
            .catch(e => ({url: url, ok: false, error: e.toString()}))
    ));
}
'''
import sys
parts = code.split('EXTRACT_FROM_HTML_JS = r"""')
if len(parts) < 2:
    print("Error splitting")
    sys.exit(1)
end_part = parts[1].split('"""', 1)[1]
code = parts[0] + 'EXTRACT_FROM_HTML_JS = r"""' + new_js + '"""' + end_part

with open('scrapers/farmaciabarata.py', 'w', encoding='utf-8') as f:
    f.write(code)
    
print('Scraper guardado en scrapers/farmaciabarata.py.')
