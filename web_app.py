import os
import sys
from flask import Flask, render_template, request, jsonify, session, redirect, url_for, flash
from sqlalchemy import text
from functools import wraps

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)
from db_models import get_engine

app = Flask(__name__)
app.secret_key = 'farmasearch_secret_key_2026'
engine = get_engine()

# ============================================================
# Credenciales de farmacias (login sencillo)
# ============================================================
FARMACIAS_USERS = {
    'dosfarma':        {'password': '1234', 'nombre': 'DosFarma'},
    'farmaciasdirect': {'password': '1234', 'nombre': 'FarmaciasDirect'},
    'promofarma':      {'password': '1234', 'nombre': 'PromoFarma'},
    'atida':           {'password': '1234', 'nombre': 'Atida'},
    'farmaciasvazquez':{'password': '1234', 'nombre': 'FarmaciasVazquez'},
}


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'farmacia_user' not in session:
            return redirect(url_for('login_page'))
        return f(*args, **kwargs)
    return decorated


# ============================================================
# Pages
# ============================================================
@app.route('/')
def index():
    return render_template('index.html')


@app.route('/mapa')
def mapa():
    return render_template('mapa.html')


@app.route('/login', methods=['GET', 'POST'])
def login_page():
    if request.method == 'POST':
        user = request.form.get('username', '').strip().lower()
        pwd = request.form.get('password', '').strip()

        if user in FARMACIAS_USERS and FARMACIAS_USERS[user]['password'] == pwd:
            session['farmacia_user'] = user
            session['farmacia_nombre'] = FARMACIAS_USERS[user]['nombre']
            return redirect(url_for('panel'))
        else:
            return render_template('login.html', error='Usuario o contraseña incorrectos')

    return render_template('login.html')


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))


@app.route('/panel')
@login_required
def panel():
    return render_template('panel.html',
                           farmacia_nombre=session['farmacia_nombre'],
                           farmacia_user=session['farmacia_user'])


# ============================================================
# API — Product search
# ============================================================
@app.route('/api/buscar')
def buscar():
    q = request.args.get('q', '').strip()
    if len(q) < 2:
        return jsonify([])

    query = text("""
        SELECT
            p.nombre, p.farmacia, p.url, p.ean,
            pr.precio, pr.en_stock
        FROM productos p
        JOIN (
            SELECT DISTINCT ON (producto_id)
                producto_id, precio, en_stock
            FROM precios
            ORDER BY producto_id, fecha_captura DESC
        ) pr ON p.id = pr.producto_id
        WHERE LOWER(p.nombre) LIKE :search
        ORDER BY p.nombre
        LIMIT 500
    """)

    search_term = f"%{q.lower()}%"

    with engine.connect() as con:
        rows = con.execute(query, {"search": search_term}).fetchall()

    productos = {}
    for row in rows:
        nombre = row.nombre.strip()
        key = nombre.lower()

        if key not in productos:
            productos[key] = {
                "nombre": nombre[:1].upper() + nombre[1:].lower() if nombre else nombre,
                "ean": row.ean if row.ean else None,
                "farmacias": []
            }

        productos[key]["farmacias"].append({
            "farmacia": row.farmacia,
            "precio": float(row.precio) if row.precio else None,
            "en_stock": bool(row.en_stock),
            "url": row.url
        })

        if row.ean and not productos[key]["ean"]:
            productos[key]["ean"] = row.ean

    result = []
    for data in productos.values():
        data["farmacias"].sort(key=lambda f: f["precio"] if f["precio"] else 9999)
        mejor_precio = next((f["precio"] for f in data["farmacias"] if f["precio"]), None)
        data["mejor_precio"] = mejor_precio
        data["num_farmacias"] = len(data["farmacias"])
        result.append(data)

    return jsonify(result)


# ============================================================
# API — Pharmacy locations
# ============================================================
@app.route('/api/farmacias/ubicaciones', methods=['GET'])
def get_ubicaciones():
    """Get all active pharmacy locations."""
    farmacia_filter = request.args.get('farmacia', '').strip()

    if farmacia_filter:
        query = text("""
            SELECT id, farmacia, nombre_tienda, direccion, latitud, longitud,
                   telefono, horario
            FROM farmacia_ubicaciones
            WHERE activa = true AND LOWER(farmacia) = :farmacia
            ORDER BY farmacia, nombre_tienda
        """)
        params = {"farmacia": farmacia_filter.lower()}
    else:
        query = text("""
            SELECT id, farmacia, nombre_tienda, direccion, latitud, longitud,
                   telefono, horario
            FROM farmacia_ubicaciones
            WHERE activa = true
            ORDER BY farmacia, nombre_tienda
        """)
        params = {}

    with engine.connect() as con:
        rows = con.execute(query, params).fetchall()

    ubicaciones = []
    for row in rows:
        ubicaciones.append({
            "id": row.id,
            "farmacia": row.farmacia,
            "nombre_tienda": row.nombre_tienda,
            "direccion": row.direccion,
            "latitud": float(row.latitud),
            "longitud": float(row.longitud),
            "telefono": row.telefono,
            "horario": row.horario
        })

    return jsonify(ubicaciones)


@app.route('/api/farmacias/ubicaciones', methods=['POST'])
@login_required
def add_ubicacion():
    """Add a pharmacy location (authenticated)."""
    data = request.get_json()

    required = ['nombre_tienda', 'direccion', 'latitud', 'longitud']
    for field in required:
        if not data.get(field):
            return jsonify({"error": f"Campo '{field}' es obligatorio"}), 400

    farmacia_nombre = session['farmacia_nombre']

    query = text("""
        INSERT INTO farmacia_ubicaciones (farmacia, nombre_tienda, direccion, latitud, longitud, telefono, horario, activa)
        VALUES (:farmacia, :nombre, :dir, :lat, :lng, :tel, :horario, true)
        RETURNING id
    """)

    with engine.connect() as con:
        result = con.execute(query, {
            "farmacia": farmacia_nombre,
            "nombre": data['nombre_tienda'],
            "dir": data['direccion'],
            "lat": data['latitud'],
            "lng": data['longitud'],
            "tel": data.get('telefono', ''),
            "horario": data.get('horario', '')
        })
        con.commit()
        new_id = result.fetchone()[0]

    return jsonify({"id": new_id, "message": "Ubicación añadida correctamente"}), 201


@app.route('/api/farmacias/ubicaciones/<int:ubicacion_id>', methods=['DELETE'])
@login_required
def delete_ubicacion(ubicacion_id):
    """Delete a pharmacy location (only own locations)."""
    farmacia_nombre = session['farmacia_nombre']

    query = text("""
        DELETE FROM farmacia_ubicaciones
        WHERE id = :id AND LOWER(farmacia) = LOWER(:farmacia)
        RETURNING id
    """)

    with engine.connect() as con:
        result = con.execute(query, {"id": ubicacion_id, "farmacia": farmacia_nombre})
        con.commit()
        deleted = result.fetchone()

    if not deleted:
        return jsonify({"error": "Ubicación no encontrada o no tienes permiso"}), 404

    return jsonify({"message": "Ubicación eliminada"})


@app.route('/api/farmacias/mis-ubicaciones')
@login_required
def mis_ubicaciones():
    """Get the logged-in pharmacy's own locations."""
    farmacia_nombre = session['farmacia_nombre']

    query = text("""
        SELECT id, nombre_tienda, direccion, latitud, longitud, telefono, horario
        FROM farmacia_ubicaciones
        WHERE LOWER(farmacia) = LOWER(:farmacia) AND activa = true
        ORDER BY nombre_tienda
    """)

    with engine.connect() as con:
        rows = con.execute(query, {"farmacia": farmacia_nombre}).fetchall()

    ubicaciones = []
    for row in rows:
        ubicaciones.append({
            "id": row.id,
            "nombre_tienda": row.nombre_tienda,
            "direccion": row.direccion,
            "latitud": float(row.latitud),
            "longitud": float(row.longitud),
            "telefono": row.telefono,
            "horario": row.horario
        })

    return jsonify(ubicaciones)


if __name__ == '__main__':
    print("\n[*] Servidor web iniciado en http://localhost:5000")
    print("    Abre esa URL en tu navegador para buscar productos.\n")
    app.run(debug=True, host='0.0.0.0', port=5000)
