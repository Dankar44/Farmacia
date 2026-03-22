-- Migration 004: Historial de productos + columnas de perfil en usuarios
-- Ejecutar en Supabase SQL Editor

-- Historial de productos consultados (vinculado a busquedas)
CREATE TABLE IF NOT EXISTS historial_productos (
    id SERIAL PRIMARY KEY,
    usuario_id INT NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
    busqueda_id INT REFERENCES historial_busquedas(id) ON DELETE CASCADE,
    nombre VARCHAR(500) NOT NULL,
    precio VARCHAR(50),
    fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_historial_productos_usuario ON historial_productos(usuario_id);
CREATE INDEX IF NOT EXISTS idx_historial_productos_busqueda ON historial_productos(busqueda_id);

-- Ampliar usuarios con datos de farmacia y preferencias
ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS farmacia_nombre VARCHAR(300);
ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS farmacia_ubicacion VARCHAR(500);
ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS farmacia_web VARCHAR(500);
ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS telefono VARCHAR(50);
ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS email_notif BOOLEAN DEFAULT FALSE;
ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS newsletter BOOLEAN DEFAULT FALSE;
