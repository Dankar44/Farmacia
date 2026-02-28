from sqlalchemy import create_engine, Column, Integer, String, Text, DECIMAL, Boolean, TIMESTAMP, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from datetime import datetime
import os
from dotenv import load_dotenv

# Cargar variables de entorno desde el archivo .env
load_dotenv()

Base = declarative_base()

class Producto(Base):
    __tablename__ = 'productos'

    id = Column(Integer, primary_key=True)
    nombre = Column(String(255), nullable=False)
    url = Column(Text, unique=True, nullable=False)
    farmacia = Column(String(100), nullable=False)
    categoria = Column(String(100))
    ean = Column(String(50))
    fecha_creacion = Column(TIMESTAMP, default=datetime.utcnow)
    
    precios = relationship("Precio", back_populates="producto", cascade="all, delete-orphan")

class Precio(Base):
    __tablename__ = 'precios'

    id = Column(Integer, primary_key=True)
    producto_id = Column(Integer, ForeignKey('productos.id', ondelete='CASCADE'))
    precio = Column(DECIMAL(10, 2), nullable=False)
    precio_original = Column(DECIMAL(10, 2))
    en_stock = Column(Boolean, default=True)
    fecha_captura = Column(TIMESTAMP, default=datetime.utcnow)
    
    producto = relationship("Producto", back_populates="precios")


class FarmaciaUbicacion(Base):
    __tablename__ = 'farmacia_ubicaciones'

    id = Column(Integer, primary_key=True)
    farmacia = Column(String(100), nullable=False)  # DosFarma, Atida, etc.
    nombre_tienda = Column(String(200), nullable=False)
    direccion = Column(String(300), nullable=False)
    latitud = Column(DECIMAL(10, 7), nullable=False)
    longitud = Column(DECIMAL(10, 7), nullable=False)
    telefono = Column(String(20))
    horario = Column(String(200))
    activa = Column(Boolean, default=True)
    fecha_creacion = Column(TIMESTAMP, default=datetime.utcnow)

def get_engine():
    # Asegúrate de crear un archivo .env en la misma carpeta con:
    # DB_USER=tu_usuario
    # DB_PASSWORD=tu_contraseña
    # DB_HOST=localhost
    # DB_PORT=5432
    # DB_NAME=farmacia_scraper_db
    
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "postgres")
    host = os.getenv("DB_HOST", "127.0.0.1")
    port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "farmacia_scraper_db")
    
    import urllib.parse
    encoded_password = urllib.parse.quote_plus(password)
    connection_string = f"postgresql://{user}:{encoded_password}@{host}:{port}/{db_name}?host=127.0.0.1"
    return create_engine(connection_string)

def init_db():
    engine = get_engine()
    # Esto creará las tablas si no existen (es otra forma de hacerlo además del script SQL)
    Base.metadata.create_all(engine)
    print("Base de datos inicializada correctamente (Tablas creadas si no existían).")
    return sessionmaker(bind=engine)

if __name__ == "__main__":
    init_db()
