# FarmaSearch - Documentacion de Scrapers

> Documento maestro con toda la informacion tecnica de scraping.
> Ultima actualizacion: 21 marzo 2026

---

## Resumen General

FarmaSearch scrapea **8 farmacias online** de España para comparar precios de parafarmacia.
Base de datos: **280,477 productos** y **327,457 registros de precios** en Supabase (PostgreSQL).

---

## Farmacias y Metodos

| Farmacia | Productos en BD | Metodo Principal | URL Base |
|---|---|---|---|
| PromoFarma | 124,047 | Sitemap + Playwright | promofarma.com |
| FarmaciasDirect | 56,882 | Empathy.co API | farmaciasdirect.com |
| FarmaciasVazquez | 38,138 | Doofinder API | farmavazquez.com |
| Atida (Mifarma) | 30,769 | Algolia API | atida.com |
| DosFarma | 30,641 | Algolia API | dosfarma.com |
| Farmacoslada | 0 (pendiente) | Sitemap + aiohttp | farmacoslada.com |
| Okfarma | 0 (pendiente) | Sitemap + Playwright | okfarma.es |
| FarmaciaBarata | 0 (pendiente) | Sitemap + Playwright | farmaciabarata.es |

---

## Sistema Multi-Nivel (3 niveles de scraping)

Cada farmacia tiene hasta 3 metodos de scraping. Si falla uno, se usa el siguiente.

| Nivel | Nombre | Que hace | Velocidad | Fiabilidad |
|---|---|---|---|---|
| N1 | API directa | Peticiones a APIs de busqueda (Algolia, Doofinder, Empathy) | Muy rapido (~1000 prods/min) | Media (si cambian la key, falla) |
| N2 | HTTP inteligente | requests + sitemap + BeautifulSoup + headers rotados + reintentos | Medio (~20 prods/min) | Alta |
| N3 | Playwright | Navegador headless completo simulando usuario real | Lento (~10 prods/min) | Muy alta |

### Disponibilidad por farmacia

| Farmacia | N1 API | N2 HTTP | N3 Playwright |
|---|---|---|---|
| Atida | ✅ Algolia API | ❌ Sitemap bloqueado por HTTP | ✅ Creado (no probado) |
| DosFarma | ✅ Algolia API | ❌ Sitemap bloqueado por HTTP | ✅ Creado (no probado) |
| FarmaciasVazquez | ✅ Doofinder API | ✅ **Probado: 20/20 OK** | ✅ Creado (no probado) |
| FarmaciasDirect | ✅ Empathy API | ✅ **Probado: 20/20 OK** | ✅ Creado (no probado) |
| PromoFarma | ❌ No tiene API | ✅ **Probado: 20/20 OK** | ✅ Ya existia |
| Farmacoslada | ❌ No tiene API | ⚠️ Existe (aiohttp, no probado) | ✅ Creado (no probado) |
| Okfarma | ❌ No tiene API | ✅ **Probado: 20/20 OK** | ✅ Ya existia |
| FarmaciaBarata | ❌ No tiene API | ✅ **Probado: 20/20 OK** | ✅ Ya existia |

### Problema conocido: Atida y DosFarma N2
Sus servidores web bloquean el acceso al sitemap via HTTP directo. El N1 (API Algolia) funciona porque las peticiones van a servidores de **algolia.net** (servicio externo), no al servidor de la farmacia. Solucion pendiente: reescribir su N2 para que use el buscador web en vez del sitemap.

---

## Estructura de Archivos

```
scrapers/
├── base_scraper.py              # Clase base con fallback automatico
├── http_utils.py                # Utilidades compartidas (headers, retry, sitemap)
│
├── atida/
│   ├── level1_api.py            # Algolia API (FUNCIONA)
│   ├── level2_http.py           # HTTP sitemap (FALLA - sitemap bloqueado)
│   └── level3_playwright.py     # Playwright (creado, no probado)
│
├── dosfarma/
│   ├── level1_api.py            # Algolia API (FUNCIONA)
│   ├── level2_http.py           # HTTP sitemap (FALLA - sitemap bloqueado)
│   └── level3_playwright.py     # Playwright (creado, no probado)
│
├── promofarma/
│   ├── level2_http.py           # HTTP + sitemap (FUNCIONA)
│   └── level3_playwright.py     # Playwright (ya existia, funciona)
│
├── vazquez/
│   ├── level1_api.py            # Doofinder API (FUNCIONA)
│   ├── level2_http.py           # HTTP + sitemap (FUNCIONA)
│   └── level3_playwright.py     # Playwright (creado, no probado)
│
├── farmaciasdirect/
│   ├── level1_api.py            # Empathy API (FUNCIONA)
│   ├── level2_http.py           # HTTP + sitemap (FUNCIONA)
│   └── level3_playwright.py     # Playwright (creado, no probado)
│
├── farmacoslada/
│   ├── level2_http.py           # aiohttp async (existia, no probado - falta aiohttp)
│   └── level3_playwright.py     # Playwright (creado, no probado)
│
├── okfarma/
│   ├── level2_http.py           # HTTP + sitemap (FUNCIONA)
│   └── level3_playwright.py     # Playwright (ya existia, funciona)
│
└── farmaciabarata/
    ├── level2_http.py           # HTTP + sitemap (FUNCIONA - 20/20 OK)
    └── level3_playwright.py     # Playwright (ya existia, funciona)
```

---

## Detalles Tecnicos por Farmacia

### Atida (Mifarma)
- **API:** Algolia (algolia.net)
- **App ID:** M8GRS7KXGP
- **Index:** atida_es_es_products
- **API Key:** Base64 codificada (publica, extraida del JavaScript de la web)
- **Renovar key:** `python scrapers/atida/level1_api.py --refresh-key` (usa Playwright para extraer la key fresca)
- **Sitemap:** https://www.atida.com/es-es/sitemap.xml (BLOQUEADO por HTTP directo)
- **Datos extraidos:** nombre, precio, precio_original, stock, EAN, marca, categoria, URL
- **Velocidad N1:** ~1000 productos/pagina, total ~30 paginas = ~5 minutos

### DosFarma
- **API:** Algolia (algolia.net)
- **App ID:** 5FYR88UN93
- **Index:** pro_dosfarma_es_products
- **API Key:** Base64 codificada
- **Renovar key:** `python scrapers/dosfarma/level1_api.py --refresh-key` (usa Playwright + Stealth)
- **Proteccion:** Cloudflare en dosfarma.com (el N2 HTTP no pasa, el N1 va a Algolia y si pasa)
- **Sitemap:** No accesible por HTTP directo
- **Velocidad N1:** Similar a Atida (~5 min)

### PromoFarma
- **Sin API publica** - Solo scraping web
- **Sitemap:** https://www.promofarma.com/es/sitemaps/index.xml (accesible, ~128K URLs)
- **Proteccion:** Client Challenge (TLS fingerprinting) - el N3 Playwright lo resuelve automaticamente
- **Datos:** nombre (data-product-name), precio (data-pvp), stock (InStock en JSON-LD)
- **Velocidad N2:** ~20 productos/min (HTTP con delays)
- **Velocidad N3:** ~50 productos/min (Playwright batch fetch 20)
- **Nota:** Es la farmacia con MAS productos (124K). Un scrape completo tarda 2-3 horas

### FarmaciasVazquez
- **API:** Doofinder (eu1-search.doofinder.com)
- **HashID:** b8385fd3e2f32aadf43c359fb6791646
- **Sitemap:** https://www.farmavazquez.com/gsitemap/2_index_sitemap.xml (accesible, ~36K URLs)
- **Estrategia N1:** Extrae IDs de producto del sitemap, luego consulta Doofinder en lotes de 100
- **Datos:** nombre, precio, precio_original, stock_quantity, EAN, marca, categoria
- **Velocidad N1:** Muy rapido (~2 min para 25K)

### FarmaciasDirect
- **API:** Empathy.co (api.empathy.co)
- **Sitemap:** https://www.farmaciasdirect.com/sitemap.xml (accesible, ~78K URLs)
- **Estrategia N1:** Busquedas alfabeticas (aa, ab, ac...) porque la API no permite "browse all"
- **Datos:** nombre, precioFinal, precioBase, marca, disponibilidad, referencia
- **Velocidad N1:** Medio (~10 min por todas las combinaciones)

### Farmacoslada
- **Sin API publica**
- **Sitemap:** https://farmacoslada.com/1_es_0_sitemap.xml (accesible, 2.4MB, ~3000 productos)
- **Plataforma:** PrestaShop
- **EAN en URL:** Los URLs tienen formato `-[EAN].html`
- **N2 actual:** Usa aiohttp (async) con 40 conexiones concurrentes
- **Dependencia:** Requiere `pip install aiohttp` (no instalado en este Mac)

### Okfarma
- **Sin API publica**
- **Sitemap:** https://okfarma.es/sitemap-1.xml (index con sub-sitemaps, ~14K URLs de productos)
- **Plataforma:** PrestaShop
- **Datos:** precio en meta product:price:amount o price_pvp en script, EAN en ean13 JSON
- **N2 probado:** 20/20 OK

### FarmaciaBarata
- **Sin API publica**
- **Sitemap:** https://www.farmaciabarata.es/module/lgsitemaps/sitemap?name=sitemap_1 (index, ~22K URLs)
- **Plataforma:** PrestaShop
- **Datos:** precio en meta product:price:amount, EAN en ean13 JSON, stock via add-to-cart button
- **N2 probado:** 20/20 OK con precios verificados contra BD existente

---

## Utilidades Compartidas (http_utils.py)

| Funcion | Que hace |
|---|---|
| `get_random_headers(referer)` | Genera headers HTTP con User-Agent aleatorio (pool de 20) |
| `fetch_with_retry(url, max_retries=3)` | GET con backoff exponencial (1s, 2s, 4s) |
| `download_sitemap(url, filter_pattern)` | Descarga y parsea XML sitemap (soporta index recursivo) |
| `extract_product_from_html(html, base_url, config)` | Extrae datos con selectores CSS configurables |
| `extract_price(text)` | Limpia y convierte texto a Decimal (maneja €, comas, etc.) |
| `random_delay(min, max)` | Espera aleatoria entre requests |

### Bug corregido
`Accept-Encoding: br` (brotli) causaba que algunos servidores devolvieran paginas vacias. Se quito brotli y se dejo solo `gzip, deflate`.

---

## Ejecucion

### Desde linea de comandos
```bash
# Un scraper especifico
python scrapers/atida/level1_api.py --limit 100
python scrapers/okfarma/level2_http.py --limit 50
python scrapers/promofarma/level3_playwright.py --limit 200

# Todos (via main.py)
python main.py atida --limit 100
python main.py todas
```

### Desde la app de escritorio (Windows)
```bash
python scraper_app.py
```
Ventana grafica con botones N1/N2/N3 por farmacia + logs en tiempo real.

### Desde la web (admin)
Panel "Scrapers" en el sidebar (solo visible para admin: danisuperk@gmail.com).
Botones Auto/N1/N2/N3 por farmacia + logs en tiempo real.

---

## Verificacion de Precios (21 marzo 2026)

Comparacion de precios scrapeados via N2 HTTP vs precios existentes en BD:

| Producto | Precio Feb 26 (BD) | Precio Mar 21 (N2) | Estado |
|---|---|---|---|
| Suavinex Esponja Bebe | 3.85 EUR | 3.69 EUR | Bajada real |
| Chicco Chupete Comfort | 7.81 EUR | 6.95 EUR | Bajada real |
| Chicco Llave Parlanchina | 16.49 EUR | 16.49 EUR | Identico |
| Chicco Portachupetes Rosa | 7.94 EUR | 7.94 EUR | Identico |
| Chicco Clip Clap | 7.07 EUR | 7.07 EUR | Identico |
| Chicco Pop Friends | 3.95 EUR | 3.95 EUR | Identico |
| Nuk cinta chupete | 7.95 EUR | 7.95 EUR | Identico |

**Conclusion:** Los precios coinciden con la BD existente. Las diferencias son bajadas de precio reales (casi 1 mes entre capturas).

---

## Pendientes

1. **Atida/DosFarma N2:** Reescribir para usar buscador web en vez de sitemap (bloqueado)
2. **Farmacoslada N2:** Probar (necesita `pip install aiohttp`)
3. **Todos los N3:** Probar en maquina con Playwright instalado (`playwright install chromium`)
4. **Maxun:** Herramienta open source (Docker) para scraping visual. No integrado. Evaluar si tiene sentido para N2 alternativo
5. **Credenciales Algolia:** Mover a .env (actualmente hardcodeadas en level1_api.py)
6. **Farmacoslada:** Tiene 0 productos en BD - necesita un scrape completo

---

## Legalidad

Scrapear precios publicos para un comparador es **legal en la UE/España**:
- Solo se extraen datos publicos (precio, nombre, stock, URL)
- Delays de 0.5-2s entre requests (no sobrecarga servidores)
- Modelo de negocio reconocido (Trivago, Kayak, Idealo, Google Shopping)
- Jurisprudencia TJUE a favor (Ryanair vs PR Aviation, Meta vs Bright Data 2024)
