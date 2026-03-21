# FarmaSearch - Guia de Automatizacion

> Como programar los scrapers para que se ejecuten solos.
> Ultima actualizacion: 21 marzo 2026

---

## Resumen

Los scrapers se pueden ejecutar automaticamente en horarios programados sin que estes delante del ordenador. Solo necesitas que el PC este encendido.

---

## Opcion 1: Windows (Programador de Tareas)

### Crear el script de ejecucion

Crea un archivo `run_scrapers.bat` en la raiz del proyecto:

```bat
@echo off
cd C:\ruta\a\FarmaSearch
echo [%date% %time%] Iniciando scrapers... >> logs\automatizacion.log

echo Ejecutando Atida N1...
python scrapers\atida\level1_api.py >> logs\automatizacion.log 2>&1

echo Ejecutando DosFarma N1...
python scrapers\dosfarma\level1_api.py >> logs\automatizacion.log 2>&1

echo Ejecutando Vazquez N1...
python scrapers\vazquez\level1_api.py >> logs\automatizacion.log 2>&1

echo Ejecutando FarmaciasDirect N1...
python scrapers\farmaciasdirect\level1_api.py >> logs\automatizacion.log 2>&1

echo Ejecutando PromoFarma N2...
python scrapers\promofarma\level2_http.py >> logs\automatizacion.log 2>&1

echo Ejecutando Okfarma N2...
python scrapers\okfarma\level2_http.py >> logs\automatizacion.log 2>&1

echo Ejecutando FarmaciaBarata N2...
python scrapers\farmaciabarata\level2_http.py >> logs\automatizacion.log 2>&1

echo Ejecutando Farmacoslada N2...
python scrapers\farmacoslada\level2_http.py >> logs\automatizacion.log 2>&1

echo [%date% %time%] Scrapers completados >> logs\automatizacion.log
```

### Programar en Windows

1. Abre **Programador de Tareas** (busca "Task Scheduler" en Windows)
2. Clic en **"Crear tarea basica"**
3. Nombre: `FarmaSearch Scrapers`
4. Desencadenador: **Semanal**, marca **Lunes**, hora **03:00**
5. Accion: **Iniciar un programa**
   - Programa: `C:\ruta\a\FarmaSearch\run_scrapers.bat`
   - Iniciar en: `C:\ruta\a\FarmaSearch`
6. En Condiciones: desmarcar "Iniciar solo si el equipo esta con alimentacion AC"
7. En Configuracion: marcar "Ejecutar tarea tan pronto como sea posible si se retrasa"
8. Guardar

### Para que se ejecute cada 2 semanas
En el paso 4, selecciona "Semanal" y pon "Repetir cada: 2 semanas".

### Verificar que funciono
Revisa el archivo `logs/automatizacion.log` para ver el resultado.

---

## Opcion 2: Mac/Linux (cron)

### Editar crontab
```bash
crontab -e
```

### Añadir la tarea
```
# FarmaSearch: scrapear todos los lunes a las 3:00 AM
0 3 * * 1 cd /ruta/a/FarmaSearch && python3 run_all.py >> logs/cron.log 2>&1
```

### Formatos comunes de cron
```
# Cada lunes a las 3:00
0 3 * * 1

# Cada 2 semanas (lunes de semana par)
0 3 * * 1 [ $(expr $(date +\%W) \% 2) -eq 0 ]

# Cada dia a las 4:00
0 4 * * *

# Primer dia de cada mes a las 2:00
0 2 1 * *
```

---

## Opcion 3: Script Python con schedule (multiplataforma)

Si prefieres no tocar cron ni el Programador de Tareas, puedes usar un script Python que se ejecuta en bucle.

### Instalar
```bash
pip install schedule
```

### Crear `scheduler.py`
```python
import schedule
import subprocess
import time
import sys
import os
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

SCRAPERS = [
    ("Atida N1", "scrapers/atida/level1_api.py"),
    ("DosFarma N1", "scrapers/dosfarma/level1_api.py"),
    ("Vazquez N1", "scrapers/vazquez/level1_api.py"),
    ("FarmaciasDirect N1", "scrapers/farmaciasdirect/level1_api.py"),
    ("PromoFarma N2", "scrapers/promofarma/level2_http.py"),
    ("Okfarma N2", "scrapers/okfarma/level2_http.py"),
    ("FarmaciaBarata N2", "scrapers/farmaciabarata/level2_http.py"),
    ("Farmacoslada N2", "scrapers/farmacoslada/level2_http.py"),
]

def run_all():
    print(f"\n{'='*60}")
    print(f"[{datetime.now()}] Iniciando scraping programado")
    print(f"{'='*60}\n")
    for name, script in SCRAPERS:
        print(f">>> {name}")
        try:
            result = subprocess.run(
                [sys.executable, os.path.join(PROJECT_ROOT, script)],
                cwd=PROJECT_ROOT,
                capture_output=True, text=True, timeout=7200
            )
            if result.returncode == 0:
                print(f"  OK")
            else:
                print(f"  ERROR (code {result.returncode})")
                print(f"  {result.stderr[-200:]}")
        except Exception as e:
            print(f"  EXCEPCION: {e}")
    print(f"\n[{datetime.now()}] Scraping completado\n")

# Programar: todos los lunes a las 3:00
schedule.every().monday.at("03:00").do(run_all)

# Alternativas:
# schedule.every(2).weeks.do(run_all)       # Cada 2 semanas
# schedule.every().day.at("04:00").do(run_all)  # Cada dia a las 4:00

print("Scheduler activo. Esperando el proximo lunes a las 3:00...")
print("(Ctrl+C para detener)")

while True:
    schedule.run_pending()
    time.sleep(60)
```

### Ejecutar
```bash
python scheduler.py
```
Dejalo corriendo en segundo plano. Ejecutara los scrapers cada lunes a las 3:00.

---

## Calendario recomendado (para evitar bloqueos)

Para distribuir la carga y no hacer demasiadas peticiones a la vez:

| Dia | Hora | Que scrapear | Metodo | Duracion aprox |
|---|---|---|---|---|
| Lunes 03:00 | Atida | N1 API | ~5 min |
| Lunes 03:10 | DosFarma | N1 API | ~5 min |
| Lunes 03:20 | FarmaciasVazquez | N1 API | ~3 min |
| Lunes 03:25 | FarmaciasDirect | N1 API | ~10 min |
| Martes 03:00 | PromoFarma | N2 HTTP | ~3 horas |
| Miercoles 03:00 | Okfarma | N2 HTTP | ~30 min |
| Miercoles 04:00 | FarmaciaBarata | N2 HTTP | ~30 min |
| Jueves 03:00 | Farmacoslada | N2 HTTP | ~15 min |

Total semanal: ~4.5 horas de scraping distribuidas en 4 noches.
Asi cada farmacia recibe maximo unas miles de peticiones repartidas en horas.

---

## Notificaciones

El script `run_all.py` ya envia un email al terminar con:
- Estado de cada scraper (OK / Error)
- Duracion total
- Backup de la BD

Configura `SMTP_USER` y `SMTP_PASS` en `.env` para recibirlo.

---

## Checklist antes de automatizar

1. [ ] Todos los scrapers probados manualmente con `--limit 20`
2. [ ] `.env` configurado con credenciales de Supabase y email
3. [ ] `requirements.txt` instalado (`pip install -r requirements.txt`)
4. [ ] Playwright instalado si se usa N3 (`playwright install chromium`)
5. [ ] Logs directory existe (`mkdir logs`)
6. [ ] Probar el script de automatizacion una vez manualmente antes de programarlo
