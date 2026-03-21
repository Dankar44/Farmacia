# Migracion a Supabase - FarmaSearch

## Resumen

Migrar la base de datos local PostgreSQL (280K productos, 327K precios, 2K ubicaciones) a Supabase para tener la BD en la nube y poder construir la web publica.

## Datos del backup

| Tabla | Filas | Descripcion |
|---|---|---|
| `productos` | 280,477 | Catalogo de productos de 8 farmacias |
| `precios` | 327,457 | Historico de precios por producto |
| `productos_consolidados` | 228,944 | Tabla derivada de comparacion cruzada |
| `farmacia_ubicaciones` | 2,159 | Ubicaciones fisicas de farmacias |

**Archivo de importacion:** `migration/supabase_import.sql.gz` (31 MB comprimido, 126 MB descomprimido)

## Paso 1: Crear proyecto en Supabase

1. Ir a https://supabase.com y crear cuenta / iniciar sesion
2. Crear nuevo proyecto:
   - **Nombre:** FarmaSearch
   - **Region:** EU West (para Espana)
   - **Password:** elegir una password segura (se usara para conectar)
3. Esperar ~2 minutos a que se inicialice

## Paso 2: Obtener la connection string

1. En el dashboard de Supabase ir a **Project Settings** (icono engranaje) > **Database**
2. En la seccion **Connection string** > pestaña **URI**, copiar la cadena. Tiene este formato:

```
postgresql://postgres.[REF]:[PASSWORD]@aws-0-eu-west-1.pooler.supabase.com:6543/postgres
```

**IMPORTANTE:** Para la importacion masiva con `psql`, usar el **connection string directo** (puerto 5432), NO el pooler (puerto 6543). Lo encuentras en:
- **Database Settings** > **Connection string** > **Session mode** (puerto 5432)

O construirlo manualmente:
```
postgresql://postgres.[REF]:[PASSWORD]@db.[REF].supabase.co:5432/postgres
```

## Paso 3: Instalar herramientas PostgreSQL (si no estan)

### macOS
```bash
brew install libpq
```
Las herramientas quedan en `/opt/homebrew/opt/libpq/bin/`

### Windows
Las herramientas vienen con la instalacion de PostgreSQL, normalmente en:
```
C:\Program Files\PostgreSQL\17\bin\
```

### Linux
```bash
sudo apt-get install postgresql-client
```

## Paso 4: Descomprimir el backup

```bash
gunzip -k migration/supabase_import.sql.gz
```

Esto genera `migration/supabase_import.sql` (126 MB).

## Paso 5: Habilitar extension unaccent en Supabase

Antes de importar, ir al **SQL Editor** en el dashboard de Supabase y ejecutar:

```sql
CREATE EXTENSION IF NOT EXISTS unaccent;
```

Esto es necesario para la funcion `normalize_product_name()`.

## Paso 6: Importar la base de datos

```bash
psql "postgresql://postgres.[REF]:[PASSWORD]@db.[REF].supabase.co:5432/postgres" < migration/supabase_import.sql
```

En macOS con libpq:
```bash
/opt/homebrew/opt/libpq/bin/psql "postgresql://postgres.[REF]:[PASSWORD]@db.[REF].supabase.co:5432/postgres" < migration/supabase_import.sql
```

Esto tardara unos minutos (~5-10 min dependiendo de la conexion).

## Paso 7: Crear la funcion de normalizacion

Despues de importar, ejecutar en el SQL Editor de Supabase:

```sql
CREATE OR REPLACE FUNCTION normalize_product_name(input_string TEXT)
RETURNS TEXT AS $$
DECLARE
    clean_str TEXT;
BEGIN
    clean_str := unaccent(lower(input_string));
    clean_str := replace(clean_str, '×', 'x');
    clean_str := replace(clean_str, '*', 'x');
    clean_str := regexp_replace(clean_str, '([0-9])([a-z])', '\1 \2', 'g');
    clean_str := regexp_replace(clean_str, '([a-z])([0-9])', '\1 \2', 'g');
    clean_str := regexp_replace(clean_str, '\y(capsulas?|caps?)\y', '', 'g');
    clean_str := regexp_replace(clean_str, '\y(comprimidos?|comp?)\y', '', 'g');
    clean_str := regexp_replace(clean_str, '\yx\y', '', 'g');
    clean_str := regexp_replace(clean_str, '\y(ml|mg|gr|g|kg|l)\y', '', 'g');
    clean_str := regexp_replace(clean_str, '\y(thea|laboratorios)\y', '', 'g');
    clean_str := regexp_replace(clean_str, '[^a-z0-9]', '', 'g');
    RETURN clean_str;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
```

## Paso 8: Crear la tabla cima_medicamentos (no esta en el backup)

```sql
CREATE TABLE IF NOT EXISTS cima_medicamentos (
    id SERIAL PRIMARY KEY,
    nregistro VARCHAR(50) NOT NULL,
    cn VARCHAR(50) UNIQUE NOT NULL,
    nombre VARCHAR(500) NOT NULL,
    pactivos VARCHAR(1000),
    labtitular VARCHAR(200),
    cpresc VARCHAR(100),
    url_foto VARCHAR(500),
    url_prospecto VARCHAR(500),
    url_ficha VARCHAR(500),
    receta BOOLEAN DEFAULT FALSE,
    fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Paso 9: Crear indices de rendimiento

```sql
CREATE INDEX IF NOT EXISTS idx_productos_url ON productos(url);
CREATE INDEX IF NOT EXISTS idx_productos_farmacia ON productos(farmacia);
CREATE INDEX IF NOT EXISTS idx_precios_producto_fecha ON precios(producto_id, fecha_captura);
CREATE INDEX IF NOT EXISTS idx_precios_fecha ON precios(fecha_captura);
```

## Paso 10: Verificar la importacion

Ejecutar en el SQL Editor:

```sql
SELECT 'productos' AS tabla, COUNT(*) AS filas FROM productos
UNION ALL
SELECT 'precios', COUNT(*) FROM precios
UNION ALL
SELECT 'productos_consolidados', COUNT(*) FROM productos_consolidados
UNION ALL
SELECT 'farmacia_ubicaciones', COUNT(*) FROM farmacia_ubicaciones;
```

Resultado esperado:
| tabla | filas |
|---|---|
| productos | 280,477 |
| precios | 327,457 |
| productos_consolidados | 228,944 |
| farmacia_ubicaciones | 2,159 |

## Paso 11: Actualizar el .env del proyecto

Cambiar las credenciales en `.env` para apuntar a Supabase:

```
DB_USER=postgres.[TU-REF]
DB_PASSWORD=[TU-PASSWORD-SUPABASE]
DB_HOST=db.[TU-REF].supabase.co
DB_PORT=5432
DB_NAME=postgres
```

## Notas importantes

- **Free Tier:** 500 MB de BD. Con los datos actuales (~250-380 MB) cabe, pero vigilar el crecimiento del historico de precios.
- **Pausa por inactividad:** El free tier pausa la BD tras 7 dias sin actividad. Para desarrollo es tolerable.
- **Pro ($25/mes):** 8 GB, sin pausas. Recomendado cuando se lance en produccion.
- **La tabla `cima_medicamentos`** no esta en el backup. Se puede popular ejecutando `scripts/descargar_cima.py` despues de configurar el `.env`.
