# FarmaSearch - Guia de Proxies y Proteccion de IP

> Como evitar que te bloqueen la IP al scrapear.
> Ultima actualizacion: 21 marzo 2026

---

## El problema

Cuando haces muchas peticiones HTTP desde la misma IP, los servidores web pueden:
- Devolver error **403 Forbidden** (bloqueado)
- Devolver error **429 Too Many Requests** (rate limit)
- Mostrarte un **CAPTCHA** en vez de la pagina
- Devolver **paginas vacias** o diferentes (como paso con el header brotli)

## Lo que ya tenemos

FarmaSearch ya tiene estas protecciones basicas:
- **Delays aleatorios** entre peticiones (0.5-2 segundos)
- **20 User-Agents rotados** (simula diferentes navegadores)
- **Reintentos con backoff exponencial** (1s, 2s, 4s entre reintentos)
- **Headers realistas** (Accept, Accept-Language, etc.)

Esto es suficiente para scraping semanal de ~280K productos.

## Cuando necesitas proxies

Si empiezas a ver errores 403/429 frecuentes, necesitas proxies rotativos.

## Servicios de proxies recomendados

| Servicio | Precio | Tipo | URL |
|---|---|---|---|
| **ScraperAPI** | $49/mes (100K peticiones) | Proxy + renders JS | scraperapi.com |
| **Smartproxy** | $14/mes (25K peticiones) | Residenciales | smartproxy.com |
| **Bright Data** | $15/mes | Residenciales + datacenter | brightdata.com |
| **ProxyScrape** | Gratis (limitado) | Datacenter | proxyscrape.com |

## Como configurar proxies en FarmaSearch

### Paso 1: Añadir credenciales al .env
```
PROXY_URL=http://usuario:password@proxy.smartproxy.com:10000
```

### Paso 2: Modificar http_utils.py
En la funcion `fetch_with_retry()`, añadir una sola linea:

```python
import os
PROXY_URL = os.getenv('PROXY_URL', '')

def fetch_with_retry(url, ...):
    proxies = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None
    response = requests.get(url, headers=headers, timeout=timeout, proxies=proxies)
```

### Paso 3: Listo
Todas las peticiones HTTP ahora salen a traves del proxy con IP rotada.
Los scrapers N1 (API directa a Algolia/Doofinder) normalmente NO necesitan proxy.
Los scrapers N2 (HTTP a webs de farmacias) son los que mas se benefician.

## Alternativa gratuita: Tor

Se puede usar la red Tor como proxy gratuito, pero es MUY lento y muchos sitios bloquean IPs de Tor.
No recomendado para scraping de produccion.

## Alternativa sin proxy: Scraping distribuido en tiempo

En vez de scrapear 30,000 productos de golpe, distribuir en el tiempo:
- Lunes 3:00: Scrapear Atida (30K prods, ~5 min via API)
- Lunes 3:10: Scrapear DosFarma (30K prods, ~5 min via API)
- Martes 3:00: Scrapear PromoFarma (124K prods, ~3h via N2)
- Miercoles 3:00: Scrapear Vazquez + FarmaciasDirect
- Jueves 3:00: Scrapear Okfarma + FarmaciaBarata + Farmacoslada

Asi nunca haces demasiadas peticiones a una misma farmacia en poco tiempo.
