-- Migration 003: Listas, comparaciones, indices para ofertas

-- Listas de productos (cestas)
CREATE TABLE IF NOT EXISTS listas (
    id SERIAL PRIMARY KEY,
    usuario_id INT NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
    nombre VARCHAR(200) NOT NULL,
    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_listas_usuario ON listas(usuario_id);

CREATE TABLE IF NOT EXISTS lista_productos (
    id SERIAL PRIMARY KEY,
    lista_id INT NOT NULL REFERENCES listas(id) ON DELETE CASCADE,
    nombre_normalizado VARCHAR(500) NOT NULL,
    nombre_display VARCHAR(500) NOT NULL,
    UNIQUE(lista_id, nombre_normalizado)
);
CREATE INDEX IF NOT EXISTS idx_lista_productos_lista ON lista_productos(lista_id);

-- Comparaciones guardadas
CREATE TABLE IF NOT EXISTS comparaciones (
    id SERIAL PRIMARY KEY,
    usuario_id INT REFERENCES usuarios(id) ON DELETE CASCADE,
    productos JSONB NOT NULL,
    fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indices para performance de ofertas y tendencias
CREATE INDEX IF NOT EXISTS idx_precios_producto_fecha_desc ON precios(producto_id, fecha_captura DESC);
CREATE INDEX IF NOT EXISTS idx_productos_nombre_norm ON productos(nombre_normalizado);

-- Actualizar precio suscripcion
UPDATE suscripciones SET precio_mensual = 50.00 WHERE precio_mensual != 50.00;
