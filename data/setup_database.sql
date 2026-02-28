-- Script para crear la estructura de la base de datos de Farmacia

-- Conéctate a tu servidor de PostgreSQL con pgAdmin y asegúrate de crear
-- una base de datos nueva (por ejemplo, llamada 'farmacia_scraper_db')
-- Luego, ejecuta este script dentro de esa base de datos.

CREATE TABLE IF NOT EXISTS productos (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR(255) NOT NULL,
    url TEXT UNIQUE NOT NULL,
    farmacia VARCHAR(100) NOT NULL,
    categoria VARCHAR(100),
    ean VARCHAR(50), -- Código de barras si logramos extraerlo
    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS precios (
    id SERIAL PRIMARY KEY,
    producto_id INTEGER REFERENCES productos(id) ON DELETE CASCADE,
    precio DECIMAL(10, 2) NOT NULL,
    precio_original DECIMAL(10, 2), -- Por si hay descuentos
    en_stock BOOLEAN DEFAULT true,
    fecha_captura TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Índices para mejorar el rendimiento de las búsquedas
CREATE INDEX IF NOT EXISTS idx_productos_url ON productos(url);
CREATE INDEX IF NOT EXISTS idx_productos_farmacia ON productos(farmacia);
CREATE INDEX IF NOT EXISTS idx_precios_producto_fecha ON precios(producto_id, fecha_captura);
CREATE INDEX IF NOT EXISTS idx_precios_fecha ON precios(fecha_captura);
