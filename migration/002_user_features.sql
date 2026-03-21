-- Migration 002: User features (favoritos, historial, alertas, suscripciones)

-- Extend usuarios table
ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS avatar_url VARCHAR(500);
ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS plan VARCHAR(20) DEFAULT 'free';
ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS plan_expira TIMESTAMP;
ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS max_favoritos INT DEFAULT 20;
ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS max_alertas INT DEFAULT 3;

-- Favoritos
CREATE TABLE IF NOT EXISTS favoritos (
    id SERIAL PRIMARY KEY,
    usuario_id INT NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
    nombre_normalizado VARCHAR(500) NOT NULL,
    nombre_display VARCHAR(500) NOT NULL,
    mejor_precio_al_guardar DECIMAL(10,2),
    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(usuario_id, nombre_normalizado)
);
CREATE INDEX IF NOT EXISTS idx_favoritos_usuario ON favoritos(usuario_id);

-- Historial de busquedas
CREATE TABLE IF NOT EXISTS historial_busquedas (
    id SERIAL PRIMARY KEY,
    usuario_id INT NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
    termino VARCHAR(300) NOT NULL,
    resultados_count INT DEFAULT 0,
    fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_historial_usuario_fecha ON historial_busquedas(usuario_id, fecha DESC);

-- Alertas de precio
CREATE TABLE IF NOT EXISTS alertas_precio (
    id SERIAL PRIMARY KEY,
    usuario_id INT NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
    nombre_normalizado VARCHAR(500) NOT NULL,
    nombre_display VARCHAR(500) NOT NULL,
    precio_objetivo DECIMAL(10,2) NOT NULL,
    precio_actual DECIMAL(10,2),
    activa BOOLEAN DEFAULT TRUE,
    notificada BOOLEAN DEFAULT FALSE,
    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_alertas_usuario ON alertas_precio(usuario_id);

-- Suscripciones
CREATE TABLE IF NOT EXISTS suscripciones (
    id SERIAL PRIMARY KEY,
    usuario_id INT NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
    plan VARCHAR(20) NOT NULL DEFAULT 'pro',
    estado VARCHAR(20) NOT NULL DEFAULT 'activa',
    metodo_pago VARCHAR(50),
    referencia_pago VARCHAR(200),
    fecha_inicio TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    fecha_fin TIMESTAMP,
    precio_mensual DECIMAL(10,2) DEFAULT 2.99
);
CREATE INDEX IF NOT EXISTS idx_suscripciones_usuario ON suscripciones(usuario_id);
