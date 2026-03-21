# FarmaSearch - Documentacion del Proyecto

> Documento maestro del proyecto completo.
> Ultima actualizacion: 21 marzo 2026

---

## Que es FarmaSearch

Comparador de precios de productos de parafarmacia en España. Scrapea 8 farmacias online, almacena precios en Supabase, y ofrece una web para buscar y comparar.

---

## Stack Tecnico

| Capa | Tecnologia |
|---|---|
| Backend | Python 3 + Flask |
| Base de datos | PostgreSQL en Supabase (nube) |
| ORM | SQLAlchemy 2.0 |
| Scraping N1 | requests (APIs: Algolia, Doofinder, Empathy) |
| Scraping N2 | requests + BeautifulSoup + headers rotados |
| Scraping N3 | Playwright (navegador headless) |
| Frontend | HTML/CSS/JS vanilla (Jinja2 templates) |
| Auth | bcrypt + Flask sessions |
| Email | Gmail SMTP |
| App escritorio | Tkinter (scraper_app.py) |

---

## Base de Datos (Supabase)

### Conexion
```
Host: aws-1-eu-west-1.pooler.supabase.com
Port: 5432
DB: postgres
User: postgres.qebstbtokosvhngubnqj
```

### Tablas principales

| Tabla | Filas | Descripcion |
|---|---|---|
| productos | 280,477 | Catalogo de productos (nombre, url, farmacia, ean) |
| precios | 327,457+ | Historico de precios (precio, stock, fecha_captura) |
| productos_consolidados | 228,944 | Tabla derivada de comparacion cruzada |
| farmacia_ubicaciones | 2,159 | Ubicaciones fisicas (lat, lng, direccion) |
| cima_medicamentos | 0 | Registro oficial de medicamentos AEMPS |
| usuarios | N | Cuentas de usuario (email, password bcrypt, plan) |
| favoritos | N | Productos guardados por usuario |
| historial_busquedas | N | Productos consultados (al expandir card) |
| alertas_precio | N | Alertas de bajada de precio |
| suscripciones | N | Registro de pagos/suscripciones |
| listas | N | Cestas de productos del usuario |
| lista_productos | N | Productos dentro de cada lista |
| scraper_runs | N | Registro de ejecuciones de scrapers |

### Migraciones
```
migration/001_initial_schema.sql     # productos, precios, indices
migration/002_user_features.sql      # usuarios, favoritos, historial, alertas, suscripciones
migration/003_features.sql           # listas, comparaciones, indices optimizados
migration/supabase_import.sql.gz     # Backup comprimido para importar
```

---

## Estructura del Proyecto

```
FarmaSearch/
├── web_app.py                # App Flask (API + auth + vistas)
├── db_models.py              # Modelos SQLAlchemy
├── main.py                   # CLI dispatcher para scrapers
├── run_all.py                # Ejecutor batch automatico
├── scraper_app.py            # App de escritorio (Tkinter)
├── requirements.txt          # Dependencias Python
├── .env                      # Credenciales (gitignored)
├── .env.example              # Plantilla de credenciales
│
├── scrapers/                 # 8 farmacias x 3 niveles
│   ├── base_scraper.py       # Clase base con fallback
│   ├── http_utils.py         # Utilidades HTTP compartidas
│   ├── atida/                # level1_api.py, level2_http.py, level3_playwright.py
│   ├── dosfarma/             # level1_api.py, level2_http.py, level3_playwright.py
│   ├── promofarma/           # level2_http.py, level3_playwright.py
│   ├── vazquez/              # level1_api.py, level2_http.py, level3_playwright.py
│   ├── farmaciasdirect/      # level1_api.py, level2_http.py, level3_playwright.py
│   ├── farmacoslada/         # level2_http.py, level3_playwright.py
│   ├── okfarma/              # level2_http.py, level3_playwright.py
│   └── farmaciabarata/       # level2_http.py, level3_playwright.py
│
├── templates/                # HTML
│   ├── index.html            # App principal (sidebar + SPA)
│   ├── landing.html          # Login/registro (50/50 split)
│   ├── login.html            # Login farmacias
│   ├── panel.html            # Admin farmacias
│   ├── mapa.html             # Mapa (oculto)
│   └── perfil.html           # Perfil (legacy)
│
├── migration/                # SQL migraciones
├── scripts/                  # Utilidades y mantenimiento
├── tests/                    # Tests
├── utils/                    # Email, contadores
├── static/                   # Favicon, imagenes
├── docs/                     # Esta documentacion
├── logs/                     # Logs scrapers (gitignored)
├── exports/                  # Excel exports (gitignored)
└── backups/                  # DB backups (gitignored)
```

---

## Funcionalidades de la Web

### Para usuarios (requiere registro)
- **Buscador** de productos con comparacion de precios entre farmacias
- **Favoritos** (guardar productos con corazon, limite 20 en estandar)
- **Historial** (productos consultados al expandir)
- **Alertas de precio** (avisame cuando baje de X EUR, limite 3 en estandar)
- **Mis Listas** (cestas de productos, comparar total por farmacia)
- **Dashboard** (metricas personales)
- **Ofertas del dia** (productos con mayor bajada de precio)
- **Tendencia de precio** (grafico Chart.js con historico por farmacia)
- **Alternativas** (productos similares mas baratos)
- **Perfil** (editar nombre, cambiar contrasena)
- **Suscripcion** (Estandar 50 EUR/mes vs Premium 70 EUR/mes)

### Para admin (solo danisuperk@gmail.com)
- **Panel Scrapers** (ejecutar/parar scrapers, ver logs en tiempo real)
- Botones por nivel (Auto, N1, N2, N3) por farmacia

### Modelo de suscripcion (todos los planes son para farmacias)
| | Estandar (50 EUR/mes) | Premium (70 EUR/mes) |
|---|---|---|
| Busqueda y comparacion | Si | Si |
| Favoritos | 20 max | Ilimitados |
| Alertas de precio | 3 max | Ilimitadas |
| Historial | Basico | Completo |
| Listas de comparacion | Si | Si |
| Ofertas y bajadas | Si | Si |
| Notificaciones email | No | Si |
| **Catalogo de productos propio** | No | **Si** |
| **Monitoreo de competencia (8 farmacias)** | No | **Si** |
| **Alertas de cambio de precio** | No | **Automaticas** |
| **Historial de cambios** | No | **Completo** |

---

## Autenticacion

### Usuarios (tabla usuarios)
- Email + password (bcrypt)
- Registro: POST /api/auth/registro
- Login: POST /api/auth/login
- Recuperar: POST /api/auth/recuperar (envia password temporal por email)
- Admin: campo `is_admin` en BD (solo danisuperk@gmail.com)

### Farmacias (hardcoded en web_app.py)
- 8 usuarios fijos (dosfarma, atida, etc.) con password "1234"
- Login en /acceso-farmacias
- Gestionan ubicaciones fisicas en /panel

---

## Despliegue

### Actual: localhost
- `python web_app.py` → http://localhost:5000
- BD en Supabase (nube)
- Scrapers en local

### Futuro: Railway/Render
- Subir codigo a GitHub
- Conectar repo en Railway ($5/mes) o Render ($7/mes)
- Configurar variables de entorno
- Instalar Playwright: `playwright install chromium`

---

## Credenciales (en .env)

```
DB_USER=postgres.qebstbtokosvhngubnqj
DB_PASSWORD=***
DB_HOST=aws-1-eu-west-1.pooler.supabase.com
DB_PORT=5432
DB_NAME=postgres
SMTP_USER=***@gmail.com
SMTP_PASS=***
EMAIL_RECEIVER=***
FLASK_SECRET=***
```

---

## Contacto

- Desarrollador: Daniel Karimi
- Email academico: daniel.karimi@alumnos.upm.es
- Universidad: UPM (Universidad Politecnica de Madrid)
- Repo: github.com/Dankar44/Farmacia
