"""
Script para descargar todas las farmacias de Madrid desde OpenStreetMap (Overpass API)
e insertarlas en la base de datos de FarmaSearch.
"""
import requests
import time
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db_models import get_engine
from sqlalchemy import text

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

# Bounding box de la Comunidad de Madrid (aprox)
# Sur, Oeste, Norte, Este
MADRID_BBOX = "40.30,-3.89,40.56,-3.52"

OVERPASS_QUERY = f"""
[out:json][timeout:60];
(
  node["amenity"="pharmacy"]({MADRID_BBOX});
  way["amenity"="pharmacy"]({MADRID_BBOX});
  relation["amenity"="pharmacy"]({MADRID_BBOX});
);
out center;
"""


def fetch_pharmacies():
    """Query Overpass API for all pharmacies in Madrid."""
    print("[1/3] Consultando Overpass API (OpenStreetMap)...")
    print(f"      Zona: Madrid ({MADRID_BBOX})")

    resp = requests.post(OVERPASS_URL, data={"data": OVERPASS_QUERY}, timeout=90)
    resp.raise_for_status()
    data = resp.json()

    elements = data.get("elements", [])
    print(f"      -> {len(elements)} farmacias encontradas en OpenStreetMap")
    return elements


def parse_pharmacies(elements):
    """Parse Overpass elements into clean pharmacy records."""
    pharmacies = []

    for el in elements:
        tags = el.get("tags", {})

        # Get coordinates
        if el["type"] == "node":
            lat = el.get("lat")
            lon = el.get("lon")
        else:
            # For ways/relations, use center point
            center = el.get("center", {})
            lat = center.get("lat")
            lon = center.get("lon")

        if not lat or not lon:
            continue

        # Build name
        name = tags.get("name", "").strip()
        if not name:
            name = tags.get("brand", "Farmacia")
        if not name:
            name = "Farmacia"

        # Build address
        street = tags.get("addr:street", "")
        housenumber = tags.get("addr:housenumber", "")
        city = tags.get("addr:city", "Madrid")
        postcode = tags.get("addr:postcode", "")

        if street:
            address = f"{street} {housenumber}".strip()
            if postcode:
                address += f", {postcode}"
            address += f", {city}"
        else:
            address = f"Madrid"

        # Phone and hours
        phone = tags.get("phone", tags.get("contact:phone", ""))
        opening_hours = tags.get("opening_hours", "")

        # Clean phone (some have multiple or weird formats)
        if phone and len(phone) > 20:
            phone = phone[:20]

        pharmacies.append({
            "nombre": name,
            "direccion": address,
            "latitud": round(lat, 7),
            "longitud": round(lon, 7),
            "telefono": phone,
            "horario": opening_hours[:200] if opening_hours else ""
        })

    print(f"[2/3] Procesadas {len(pharmacies)} farmacias válidas")
    return pharmacies


def insert_pharmacies(pharmacies):
    """Insert pharmacies into the database."""
    engine = get_engine()

    with engine.connect() as con:
        # Clear old data
        result = con.execute(text("DELETE FROM farmacia_ubicaciones"))
        print(f"      Eliminadas {result.rowcount} ubicaciones previas")

        # Insert new
        query = text("""
            INSERT INTO farmacia_ubicaciones
                (farmacia, nombre_tienda, direccion, latitud, longitud, telefono, horario, activa)
            VALUES
                (:farmacia, :nombre, :dir, :lat, :lng, :tel, :horario, true)
        """)

        count = 0
        for p in pharmacies:
            con.execute(query, {
                "farmacia": "OpenStreetMap",
                "nombre": p["nombre"],
                "dir": p["direccion"],
                "lat": p["latitud"],
                "lng": p["longitud"],
                "tel": p["telefono"],
                "horario": p["horario"]
            })
            count += 1

        con.commit()

    print(f"[3/3] {count} farmacias insertadas en la base de datos OK")
    print(f"\n      Abre http://localhost:5000/mapa para verlas en el mapa")


if __name__ == "__main__":
    elements = fetch_pharmacies()
    pharmacies = parse_pharmacies(elements)
    insert_pharmacies(pharmacies)
