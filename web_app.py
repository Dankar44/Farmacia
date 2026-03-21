import os
import sys
import re
import bcrypt
from datetime import datetime, timezone
from flask import Flask, render_template, request, jsonify, session, redirect, url_for
from sqlalchemy import text
from functools import wraps

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)
from db_models import get_engine

app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET', 'farmasearch_secret_key_2026_prod')
engine = get_engine()


# ============================================================
# Auth helpers
# ============================================================
def hash_password(password):
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def check_password(password, hashed):
    return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))

def user_required(f):
    """Decorator: requires a logged-in user (from usuarios table)."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('landing'))
        return f(*args, **kwargs)
    return decorated

def admin_required(f):
    """Decorator: requires admin user."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session:
            return jsonify({"error": "No autorizado"}), 401
        if not session.get('is_admin', False):
            return jsonify({"error": "Acceso solo para administradores"}), 403
        return f(*args, **kwargs)
    return decorated


def farmacia_required(f):
    """Decorator: requires a logged-in pharmacy (from FARMACIAS_USERS)."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'farmacia_user' not in session:
            return redirect(url_for('farmacia_login_page'))
        return f(*args, **kwargs)
    return decorated


# Pharmacy credentials (separate from user auth)
FARMACIAS_USERS = {
    'dosfarma':        {'password': '1234', 'nombre': 'DosFarma'},
    'farmaciasdirect': {'password': '1234', 'nombre': 'FarmaciasDirect'},
    'promofarma':      {'password': '1234', 'nombre': 'PromoFarma'},
    'atida':           {'password': '1234', 'nombre': 'Atida'},
    'farmaciasvazquez':{'password': '1234', 'nombre': 'FarmaciasVazquez'},
    'farmacoslada':    {'password': '1234', 'nombre': 'Farmacoslada'},
    'okfarma':         {'password': '1234', 'nombre': 'Okfarma'},
    'farmaciabarata':  {'password': '1234', 'nombre': 'FarmaciaBarata'},
}


# ============================================================
# Pages — User flow
# ============================================================
@app.route('/')
def landing():
    """Landing page: if logged in → comparator, else → landing with auth."""
    if 'user_id' in session:
        plan = 'estandar'
        try:
            with engine.connect() as con:
                row = con.execute(text("SELECT plan FROM usuarios WHERE id = :id"), {"id": session['user_id']}).fetchone()
                if row: plan = row.plan
        except: pass
        return render_template('index.html', user_nombre=session.get('user_nombre', ''), user_plan=plan)
    return render_template('landing.html')


@app.route('/buscar')
@user_required
def buscar_page():
    """Search/comparator page (requires auth)."""
    return render_template('index.html', user_nombre=session.get('user_nombre', ''))


@app.route('/mapa')
def mapa():
    return render_template('mapa.html')


# ============================================================
# Auth — User registration & login
# ============================================================
@app.route('/api/auth/registro', methods=['POST'])
def api_registro():
    data = request.get_json()
    nombre = (data.get('nombre') or '').strip()
    email = (data.get('email') or '').strip().lower()
    password = (data.get('password') or '')

    # Validation
    if not nombre or len(nombre) < 2:
        return jsonify({"error": "El nombre debe tener al menos 2 caracteres"}), 400
    if not email or not re.match(r'^[^@]+@[^@]+\.[^@]+$', email):
        return jsonify({"error": "Email no valido"}), 400
    if len(password) < 6:
        return jsonify({"error": "La contrasena debe tener al menos 6 caracteres"}), 400

    pw_hash = hash_password(password)

    try:
        with engine.connect() as con:
            # Check if email already exists
            existing = con.execute(
                text("SELECT id FROM usuarios WHERE email = :email"),
                {"email": email}
            ).fetchone()
            if existing:
                return jsonify({"error": "Ya existe una cuenta con este email"}), 409

            result = con.execute(
                text("""
                    INSERT INTO usuarios (nombre, email, password_hash)
                    VALUES (:nombre, :email, :pw_hash)
                    RETURNING id
                """),
                {"nombre": nombre, "email": email, "pw_hash": pw_hash}
            )
            con.commit()
            user_id = result.fetchone()[0]

        # Auto-login after registration
        session['user_id'] = user_id
        session['user_nombre'] = nombre
        session['user_email'] = email

        return jsonify({"message": "Cuenta creada correctamente", "redirect": "/"}), 201

    except Exception as e:
        return jsonify({"error": "Error al crear la cuenta"}), 500


@app.route('/api/auth/login', methods=['POST'])
def api_login():
    data = request.get_json()
    email = (data.get('email') or '').strip().lower()
    password = (data.get('password') or '')

    if not email or not password:
        return jsonify({"error": "Email y contrasena son obligatorios"}), 400

    with engine.connect() as con:
        row = con.execute(
            text("SELECT id, nombre, email, password_hash, activo, is_admin FROM usuarios WHERE email = :email"),
            {"email": email}
        ).fetchone()

    if not row:
        return jsonify({"error": "Email o contrasena incorrectos"}), 401

    if not row.activo:
        return jsonify({"error": "Cuenta desactivada. Contacta con soporte."}), 403

    if not check_password(password, row.password_hash):
        return jsonify({"error": "Email o contrasena incorrectos"}), 401

    # Update last login
    with engine.connect() as con:
        con.execute(
            text("UPDATE usuarios SET ultimo_login = :now WHERE id = :id"),
            {"now": datetime.now(timezone.utc), "id": row.id}
        )
        con.commit()

    session['user_id'] = row.id
    session['user_nombre'] = row.nombre
    session['user_email'] = row.email
    session['is_admin'] = bool(row.is_admin)

    return jsonify({"message": "Sesion iniciada", "redirect": "/", "nombre": row.nombre})


@app.route('/api/auth/recuperar', methods=['POST'])
def api_recuperar():
    """Password recovery: generates a temporary password and emails it."""
    data = request.get_json()
    email = (data.get('email') or '').strip().lower()

    if not email:
        return jsonify({"error": "Introduce tu email"}), 400

    with engine.connect() as con:
        row = con.execute(
            text("SELECT id, nombre FROM usuarios WHERE email = :email"),
            {"email": email}
        ).fetchone()

    if not row:
        # Don't reveal whether the email exists
        return jsonify({"message": "Si el email existe, recibiras instrucciones para recuperar tu contrasena."})

    # Generate a temporary password
    import secrets
    temp_password = secrets.token_urlsafe(8)
    new_hash = hash_password(temp_password)

    with engine.connect() as con:
        con.execute(
            text("UPDATE usuarios SET password_hash = :pw WHERE id = :id"),
            {"pw": new_hash, "id": row.id}
        )
        con.commit()

    # Send email
    try:
        from utils.email_utils import send_email
        send_email(
            subject="FarmaSearch - Recuperar contrasena",
            body=f"""
            <h2>Hola {row.nombre},</h2>
            <p>Tu nueva contrasena temporal es:</p>
            <p style="font-size:1.5rem;font-weight:bold;background:#f0f0f0;padding:12px;border-radius:8px;text-align:center">{temp_password}</p>
            <p>Usa esta contrasena para iniciar sesion y luego cambiala desde tu perfil.</p>
            <p><small>Si no solicitaste esto, ignora este email.</small></p>
            """,
            to_email=email
        )
    except Exception:
        pass  # Email might fail but password is already reset

    return jsonify({"message": "Si el email existe, recibiras instrucciones para recuperar tu contrasena."})


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('landing'))


# ============================================================
# Context processor — inject user data into all templates
# ============================================================
@app.context_processor
def inject_user():
    if 'user_id' in session:
        return {
            'logged_in': True,
            'user_nombre': session.get('user_nombre', ''),
            'user_email': session.get('user_email', ''),
            'user_id': session.get('user_id'),
            'is_admin': session.get('is_admin', False)
        }
    return {'logged_in': False, 'is_admin': False}


# ============================================================
# API — Favorites
# ============================================================
@app.route('/api/favoritos')
@user_required
def get_favoritos():
    """Get user's favorites list."""
    with engine.connect() as con:
        rows = con.execute(text("""
            SELECT f.id, f.nombre_normalizado, f.nombre_display, f.mejor_precio_al_guardar, f.fecha_creacion
            FROM favoritos f WHERE f.usuario_id = :uid ORDER BY f.fecha_creacion DESC
        """), {"uid": session['user_id']}).fetchall()
    return jsonify([{
        "id": r.id, "nombre_normalizado": r.nombre_normalizado,
        "nombre_display": r.nombre_display, "precio_guardado": float(r.mejor_precio_al_guardar) if r.mejor_precio_al_guardar else None,
        "fecha": r.fecha_creacion.isoformat() if r.fecha_creacion else None
    } for r in rows])

@app.route('/api/favoritos', methods=['POST'])
@user_required
def add_favorito():
    data = request.get_json()
    nombre_norm = (data.get('nombre_normalizado') or '').strip()
    nombre_display = (data.get('nombre_display') or '').strip()
    precio = data.get('mejor_precio')
    if not nombre_norm or not nombre_display:
        return jsonify({"error": "Datos incompletos"}), 400

    with engine.connect() as con:
        # Check plan limits
        user = con.execute(text("SELECT plan, max_favoritos FROM usuarios WHERE id = :id"), {"id": session['user_id']}).fetchone()
        if user.plan != 'premium':
            count = con.execute(text("SELECT COUNT(*) FROM favoritos WHERE usuario_id = :id"), {"id": session['user_id']}).scalar()
            if count >= user.max_favoritos:
                return jsonify({"error": "Limite de favoritos alcanzado. Actualiza a Pro para guardar mas.", "limit_reached": True}), 403

        try:
            con.execute(text("""
                INSERT INTO favoritos (usuario_id, nombre_normalizado, nombre_display, mejor_precio_al_guardar)
                VALUES (:uid, :key, :name, :price)
                ON CONFLICT (usuario_id, nombre_normalizado) DO NOTHING
            """), {"uid": session['user_id'], "key": nombre_norm, "name": nombre_display, "price": precio})
            con.commit()
        except Exception:
            return jsonify({"error": "Error al guardar"}), 500
    return jsonify({"message": "Guardado en favoritos"}), 201

@app.route('/api/favoritos/<path:nombre_norm>', methods=['DELETE'])
@user_required
def delete_favorito(nombre_norm):
    with engine.connect() as con:
        con.execute(text("DELETE FROM favoritos WHERE usuario_id = :uid AND nombre_normalizado = :key"),
                    {"uid": session['user_id'], "key": nombre_norm})
        con.commit()
    return jsonify({"message": "Eliminado de favoritos"})

@app.route('/api/favoritos/keys')
@user_required
def get_favorito_keys():
    """Get just the normalized keys for checking hearts in search results."""
    with engine.connect() as con:
        rows = con.execute(text("SELECT nombre_normalizado FROM favoritos WHERE usuario_id = :uid"),
                          {"uid": session['user_id']}).fetchall()
    return jsonify([r.nombre_normalizado for r in rows])


# ============================================================
# Pages — Pharmacy management (separate auth)
# ============================================================
@app.route('/acceso-farmacias', methods=['GET', 'POST'])
def farmacia_login_page():
    if request.method == 'POST':
        user = request.form.get('username', '').strip().lower()
        pwd = request.form.get('password', '').strip()
        if user in FARMACIAS_USERS and FARMACIAS_USERS[user]['password'] == pwd:
            session['farmacia_user'] = user
            session['farmacia_nombre'] = FARMACIAS_USERS[user]['nombre']
            return redirect(url_for('panel'))
        else:
            return render_template('login.html', error='Usuario o contrasena incorrectos')
    return render_template('login.html')


@app.route('/panel')
@farmacia_required
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
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 50))
    sort = request.args.get('sort', 'relevancia')

    if len(q) < 2:
        return jsonify({"total": 0, "items": []})

    offset = (page - 1) * limit

    order_clause = "key_name ASC"
    if sort == 'nombre-desc':
        order_clause = "key_name DESC"
    elif sort == 'precio-asc':
        order_clause = "mejor_precio ASC NULLS LAST, key_name ASC"
    elif sort == 'precio-desc':
        order_clause = "mejor_precio DESC NULLS LAST, key_name ASC"
    elif sort == 'farmacias-desc':
        order_clause = "num_farmacias DESC, key_name ASC"
    elif sort == 'stock':
        order_clause = "tiene_stock DESC, key_name ASC"

    count_query = text("""
        SELECT COUNT(DISTINCT nombre_normalizado)
        FROM productos
        WHERE nombre_normalizado LIKE '%' || normalize_product_name(:search_raw) || '%'
    """)

    keys_query = text(f"""
        WITH latest_prices AS (
            SELECT DISTINCT ON (producto_id)
                producto_id, precio, en_stock
            FROM precios
            ORDER BY producto_id, fecha_captura DESC
        ),
        product_stats AS (
            SELECT
                p.nombre_normalizado as key_name,
                MIN(pr.precio) as mejor_precio,
                COUNT(p.id) as num_farmacias,
                BOOL_OR(pr.en_stock) as tiene_stock,
                (SELECT nombre FROM productos p2 WHERE p2.nombre_normalizado = p.nombre_normalizado ORDER BY LENGTH(nombre) ASC LIMIT 1) as display_name
            FROM productos p
            LEFT JOIN latest_prices pr ON p.id = pr.producto_id
            WHERE p.nombre_normalizado LIKE '%' || normalize_product_name(:search_raw) || '%'
            GROUP BY p.nombre_normalizado
        )
        SELECT key_name, display_name FROM product_stats
        ORDER BY {order_clause}
        LIMIT :limit OFFSET :offset
    """)

    with engine.connect() as con:
        count = con.execute(count_query, {"search_raw": q}).scalar()
        if count == 0:
            return jsonify({"total": 0, "items": []})

        keys_and_names = con.execute(keys_query, {"search_raw": q, "limit": limit, "offset": offset}).fetchall()
        keys = [row.key_name for row in keys_and_names]
        display_names = {row.key_name: row.display_name for row in keys_and_names}

        if not keys:
            return jsonify({"total": count, "items": []})

        data_query = text("""
            SELECT p.nombre_normalizado, p.farmacia, p.url, p.ean, pr.precio, pr.en_stock
            FROM productos p
            LEFT JOIN (
                SELECT DISTINCT ON (producto_id) producto_id, precio, en_stock
                FROM precios ORDER BY producto_id, fecha_captura DESC
            ) pr ON p.id = pr.producto_id
            WHERE p.nombre_normalizado = ANY(:keys)
        """)
        rows = con.execute(data_query, {"keys": keys}).fetchall()

    productos = {}
    for row in rows:
        key = row.nombre_normalizado
        if key not in productos:
            d_name = display_names.get(key, "Producto Desconocido")
            productos[key] = {
                "nombre": d_name[:1].upper() + d_name[1:].lower(),
                "ean": row.ean if row.ean else None,
                "farmacias": []
            }
        productos[key]["farmacias"].append({
            "farmacia": row.farmacia,
            "precio": float(row.precio) if row.precio else None,
            "en_stock": bool(row.en_stock) if row.en_stock is not None else False,
            "url": row.url
        })
        if row.ean and not productos[key]["ean"]:
            productos[key]["ean"] = row.ean

    result = []
    for key in keys:
        if key in productos:
            data = productos[key]
            data["farmacias"].sort(key=lambda f: f["precio"] if f["precio"] else 9999)
            mejor_precio = next((f["precio"] for f in data["farmacias"] if f["precio"]), None)
            data["mejor_precio"] = mejor_precio
            data["num_farmacias"] = len(data["farmacias"])
            result.append(data)

    return jsonify({"total": count, "items": result})


# ============================================================
# API — Product View History (when user expands a product card)
# ============================================================
@app.route('/api/historial/producto', methods=['POST'])
@user_required
def registrar_producto_visto():
    """Record when user expands/views a product card."""
    data = request.get_json()
    nombre = (data.get('nombre') or '').strip()
    precio = data.get('mejor_precio')
    num_farmacias = data.get('num_farmacias', 0)
    if not nombre:
        return jsonify({"error": "Nombre requerido"}), 400
    try:
        with engine.connect() as con:
            con.execute(text("""
                INSERT INTO historial_busquedas (usuario_id, termino, resultados_count)
                VALUES (:uid, :nombre, :farmacias)
            """), {"uid": session['user_id'], "nombre": nombre, "farmacias": num_farmacias})
            con.commit()
    except Exception:
        pass
    return jsonify({"ok": True})

@app.route('/api/historial')
@user_required
def get_historial():
    limit = int(request.args.get('limit', 50))
    with engine.connect() as con:
        rows = con.execute(text("""
            SELECT DISTINCT ON (termino) id, termino, resultados_count, fecha
            FROM historial_busquedas WHERE usuario_id = :uid
            ORDER BY termino, fecha DESC
            LIMIT :limit
        """), {"uid": session['user_id'], "limit": limit}).fetchall()
    items = sorted([{"id": r.id, "nombre": r.termino, "num_farmacias": r.resultados_count,
                     "fecha": r.fecha.isoformat() if r.fecha else None} for r in rows],
                   key=lambda x: x['fecha'] or '', reverse=True)
    return jsonify(items)

@app.route('/api/historial', methods=['DELETE'])
@user_required
def clear_historial():
    with engine.connect() as con:
        con.execute(text("DELETE FROM historial_busquedas WHERE usuario_id = :uid"), {"uid": session['user_id']})
        con.commit()
    return jsonify({"message": "Historial borrado"})


# ============================================================
# API — Profile
# ============================================================
@app.route('/perfil')
@user_required
def perfil():
    with engine.connect() as con:
        user = con.execute(text("SELECT id, nombre, email, plan, plan_expira, max_favoritos, max_alertas, fecha_registro FROM usuarios WHERE id = :id"),
                          {"id": session['user_id']}).fetchone()
        fav_count = con.execute(text("SELECT COUNT(*) FROM favoritos WHERE usuario_id = :id"), {"id": session['user_id']}).scalar()
        alert_count = con.execute(text("SELECT COUNT(*) FROM alertas_precio WHERE usuario_id = :id AND activa = true"), {"id": session['user_id']}).scalar()
    return render_template('perfil.html',
        user_nombre=user.nombre, user_email=user.email, user_plan=user.plan,
        plan_expira=user.plan_expira, max_favoritos=user.max_favoritos,
        max_alertas=user.max_alertas, fav_count=fav_count, alert_count=alert_count,
        fecha_registro=user.fecha_registro)

@app.route('/api/suscripcion')
@user_required
def get_suscripcion():
    """Get user's subscription info."""
    with engine.connect() as con:
        user = con.execute(text(
            "SELECT plan, plan_expira, max_favoritos, max_alertas, fecha_registro FROM usuarios WHERE id = :id"
        ), {"id": session['user_id']}).fetchone()
        fav_count = con.execute(text("SELECT COUNT(*) FROM favoritos WHERE usuario_id = :id"), {"id": session['user_id']}).scalar()
    return jsonify({
        "plan": user.plan or 'estandar',
        "plan_expira": user.plan_expira.isoformat() if user.plan_expira else None,
        "max_favoritos": user.max_favoritos,
        "fav_count": fav_count,
        "precio_mensual": 50.00,
        "fecha_registro": user.fecha_registro.isoformat() if user.fecha_registro else None
    })


@app.route('/api/perfil/actualizar', methods=['POST'])
@user_required
def actualizar_perfil():
    data = request.get_json()
    nombre = (data.get('nombre') or '').strip()
    if not nombre or len(nombre) < 2:
        return jsonify({"error": "Nombre debe tener al menos 2 caracteres"}), 400
    with engine.connect() as con:
        con.execute(text("UPDATE usuarios SET nombre = :nombre WHERE id = :id"),
                   {"nombre": nombre, "id": session['user_id']})
        con.commit()
    session['user_nombre'] = nombre
    return jsonify({"message": "Perfil actualizado"})

@app.route('/api/perfil/cambiar-password', methods=['POST'])
@user_required
def cambiar_password():
    data = request.get_json()
    current_pw = data.get('current_password', '')
    new_pw = data.get('new_password', '')
    if len(new_pw) < 6:
        return jsonify({"error": "La nueva contrasena debe tener al menos 6 caracteres"}), 400
    with engine.connect() as con:
        row = con.execute(text("SELECT password_hash FROM usuarios WHERE id = :id"), {"id": session['user_id']}).fetchone()
        if not check_password(current_pw, row.password_hash):
            return jsonify({"error": "Contrasena actual incorrecta"}), 401
        con.execute(text("UPDATE usuarios SET password_hash = :pw WHERE id = :id"),
                   {"pw": hash_password(new_pw), "id": session['user_id']})
        con.commit()
    return jsonify({"message": "Contrasena cambiada"})


# ============================================================
# API — Price Alerts
# ============================================================
@app.route('/api/alertas')
@user_required
def get_alertas():
    with engine.connect() as con:
        rows = con.execute(text("""
            SELECT a.id, a.nombre_normalizado, a.nombre_display, a.precio_objetivo,
                   a.precio_actual, a.activa, a.notificada, a.fecha_creacion
            FROM alertas_precio a WHERE a.usuario_id = :uid ORDER BY a.fecha_creacion DESC
        """), {"uid": session['user_id']}).fetchall()
    return jsonify([{
        "id": r.id, "nombre_normalizado": r.nombre_normalizado,
        "nombre_display": r.nombre_display, "precio_objetivo": float(r.precio_objetivo),
        "precio_actual": float(r.precio_actual) if r.precio_actual else None,
        "activa": r.activa, "notificada": r.notificada,
        "fecha": r.fecha_creacion.isoformat() if r.fecha_creacion else None
    } for r in rows])

@app.route('/api/alertas', methods=['POST'])
@user_required
def add_alerta():
    data = request.get_json()
    nombre_norm = (data.get('nombre_normalizado') or '').strip()
    nombre_display = (data.get('nombre_display') or '').strip()
    precio_obj = data.get('precio_objetivo')
    if not nombre_norm or not nombre_display or not precio_obj:
        return jsonify({"error": "Datos incompletos"}), 400
    with engine.connect() as con:
        user = con.execute(text("SELECT plan, max_alertas FROM usuarios WHERE id = :id"), {"id": session['user_id']}).fetchone()
        if user.plan != 'premium':
            count = con.execute(text("SELECT COUNT(*) FROM alertas_precio WHERE usuario_id = :id AND activa = true"), {"id": session['user_id']}).scalar()
            if count >= user.max_alertas:
                return jsonify({"error": "Limite de alertas alcanzado. Actualiza a Pro.", "limit_reached": True}), 403
        con.execute(text("""
            INSERT INTO alertas_precio (usuario_id, nombre_normalizado, nombre_display, precio_objetivo)
            VALUES (:uid, :key, :name, :price)
        """), {"uid": session['user_id'], "key": nombre_norm, "name": nombre_display, "price": precio_obj})
        con.commit()
    return jsonify({"message": "Alerta creada"}), 201

@app.route('/api/alertas/<int:alerta_id>', methods=['DELETE'])
@user_required
def delete_alerta(alerta_id):
    with engine.connect() as con:
        con.execute(text("DELETE FROM alertas_precio WHERE id = :id AND usuario_id = :uid"),
                    {"id": alerta_id, "uid": session['user_id']})
        con.commit()
    return jsonify({"message": "Alerta eliminada"})

@app.route('/api/alertas/<int:alerta_id>', methods=['PUT'])
@user_required
def update_alerta(alerta_id):
    data = request.get_json()
    precio = data.get('precio_objetivo')
    if not precio:
        return jsonify({"error": "Precio requerido"}), 400
    with engine.connect() as con:
        con.execute(text("UPDATE alertas_precio SET precio_objetivo = :price WHERE id = :id AND usuario_id = :uid"),
                    {"price": precio, "id": alerta_id, "uid": session['user_id']})
        con.commit()
    return jsonify({"message": "Alerta actualizada"})

@app.route('/api/alertas/keys')
@user_required
def get_alerta_keys():
    with engine.connect() as con:
        rows = con.execute(text("SELECT nombre_normalizado FROM alertas_precio WHERE usuario_id = :uid AND activa = true"),
                          {"uid": session['user_id']}).fetchall()
    return jsonify([r.nombre_normalizado for r in rows])


# ============================================================
# API — Price Trends
# ============================================================
@app.route('/api/producto/tendencia')
def get_tendencia():
    key = request.args.get('nombre_normalizado', '').strip()
    if not key:
        return jsonify({"error": "nombre_normalizado requerido"}), 400
    with engine.connect() as con:
        rows = con.execute(text("""
            SELECT p.farmacia, pr.precio, pr.fecha_captura
            FROM productos p JOIN precios pr ON p.id = pr.producto_id
            WHERE p.nombre_normalizado = :key
            ORDER BY pr.fecha_captura
        """), {"key": key}).fetchall()
    # Group by farmacia
    farmacias = {}
    for r in rows:
        if r.farmacia not in farmacias:
            farmacias[r.farmacia] = []
        farmacias[r.farmacia].append({
            "precio": float(r.precio),
            "fecha": r.fecha_captura.strftime('%Y-%m-%d') if r.fecha_captura else None
        })
    return jsonify(farmacias)


# ============================================================
# API — Daily Deals (price drops)
# ============================================================
@app.route('/api/ofertas')
def get_ofertas():
    limit = int(request.args.get('limit', 20))
    with engine.connect() as con:
        rows = con.execute(text("""
            WITH ranked AS (
                SELECT producto_id, precio, fecha_captura,
                       ROW_NUMBER() OVER (PARTITION BY producto_id ORDER BY fecha_captura DESC) as rn
                FROM precios WHERE precio > 0
            ),
            latest AS (SELECT producto_id, precio as precio_actual FROM ranked WHERE rn = 1),
            previous AS (SELECT producto_id, precio as precio_anterior FROM ranked WHERE rn = 2)
            SELECT p.nombre, p.farmacia, p.url, p.nombre_normalizado,
                   l.precio_actual, v.precio_anterior,
                   ROUND((1 - l.precio_actual / NULLIF(v.precio_anterior, 0)) * 100, 1) as descuento_pct
            FROM latest l
            JOIN previous v ON l.producto_id = v.producto_id
            JOIN productos p ON p.id = l.producto_id
            WHERE v.precio_anterior > l.precio_actual
              AND l.precio_actual > 0
              AND v.precio_anterior > 0
              AND (v.precio_anterior - l.precio_actual) >= 0.50
            ORDER BY descuento_pct DESC
            LIMIT :limit
        """), {"limit": limit}).fetchall()
    return jsonify([{
        "nombre": r.nombre, "farmacia": r.farmacia, "url": r.url,
        "nombre_normalizado": r.nombre_normalizado,
        "precio_actual": float(r.precio_actual), "precio_anterior": float(r.precio_anterior),
        "descuento_pct": float(r.descuento_pct)
    } for r in rows])


# ============================================================
# API — Shopping Lists
# ============================================================
@app.route('/api/listas')
@user_required
def get_listas():
    with engine.connect() as con:
        rows = con.execute(text("""
            SELECT l.id, l.nombre, l.fecha_creacion,
                   (SELECT COUNT(*) FROM lista_productos lp WHERE lp.lista_id = l.id) as num_productos
            FROM listas l WHERE l.usuario_id = :uid ORDER BY l.fecha_creacion DESC
        """), {"uid": session['user_id']}).fetchall()
    return jsonify([{
        "id": r.id, "nombre": r.nombre, "num_productos": r.num_productos,
        "fecha": r.fecha_creacion.isoformat() if r.fecha_creacion else None
    } for r in rows])

@app.route('/api/listas', methods=['POST'])
@user_required
def create_lista():
    data = request.get_json()
    nombre = (data.get('nombre') or '').strip()
    if not nombre:
        return jsonify({"error": "Nombre requerido"}), 400
    with engine.connect() as con:
        result = con.execute(text("INSERT INTO listas (usuario_id, nombre) VALUES (:uid, :nombre) RETURNING id"),
                            {"uid": session['user_id'], "nombre": nombre})
        con.commit()
        return jsonify({"id": result.fetchone()[0], "message": "Lista creada"}), 201

@app.route('/api/listas/<int:lista_id>', methods=['DELETE'])
@user_required
def delete_lista(lista_id):
    with engine.connect() as con:
        con.execute(text("DELETE FROM listas WHERE id = :id AND usuario_id = :uid"),
                    {"id": lista_id, "uid": session['user_id']})
        con.commit()
    return jsonify({"message": "Lista eliminada"})

@app.route('/api/listas/<int:lista_id>/producto', methods=['POST'])
@user_required
def add_to_lista(lista_id):
    data = request.get_json()
    nombre_norm = (data.get('nombre_normalizado') or '').strip()
    nombre_display = (data.get('nombre_display') or '').strip()
    if not nombre_norm or not nombre_display:
        return jsonify({"error": "Datos incompletos"}), 400
    with engine.connect() as con:
        # Verify ownership
        owner = con.execute(text("SELECT id FROM listas WHERE id = :id AND usuario_id = :uid"),
                           {"id": lista_id, "uid": session['user_id']}).fetchone()
        if not owner:
            return jsonify({"error": "Lista no encontrada"}), 404
        con.execute(text("""
            INSERT INTO lista_productos (lista_id, nombre_normalizado, nombre_display)
            VALUES (:lid, :key, :name) ON CONFLICT DO NOTHING
        """), {"lid": lista_id, "key": nombre_norm, "name": nombre_display})
        con.commit()
    return jsonify({"message": "Producto añadido"}), 201

@app.route('/api/listas/<int:lista_id>/producto/<path:nombre_norm>', methods=['DELETE'])
@user_required
def remove_from_lista(lista_id, nombre_norm):
    with engine.connect() as con:
        con.execute(text("""
            DELETE FROM lista_productos WHERE lista_id = :lid AND nombre_normalizado = :key
            AND lista_id IN (SELECT id FROM listas WHERE usuario_id = :uid)
        """), {"lid": lista_id, "key": nombre_norm, "uid": session['user_id']})
        con.commit()
    return jsonify({"message": "Producto eliminado de la lista"})

@app.route('/api/listas/<int:lista_id>')
@user_required
def get_lista_detail(lista_id):
    with engine.connect() as con:
        lista = con.execute(text("SELECT id, nombre FROM listas WHERE id = :id AND usuario_id = :uid"),
                           {"id": lista_id, "uid": session['user_id']}).fetchone()
        if not lista:
            return jsonify({"error": "Lista no encontrada"}), 404
        productos = con.execute(text("""
            SELECT lp.nombre_normalizado, lp.nombre_display FROM lista_productos lp WHERE lp.lista_id = :lid
        """), {"lid": lista_id}).fetchall()
        if not productos:
            return jsonify({"id": lista.id, "nombre": lista.nombre, "productos": [], "totales": {}})
        keys = [p.nombre_normalizado for p in productos]
        # Get current prices for all products in the list
        prices = con.execute(text("""
            SELECT p.nombre_normalizado, p.farmacia, pr.precio, pr.en_stock
            FROM productos p
            JOIN (SELECT DISTINCT ON (producto_id) producto_id, precio, en_stock FROM precios ORDER BY producto_id, fecha_captura DESC) pr
            ON p.id = pr.producto_id
            WHERE p.nombre_normalizado = ANY(:keys) AND pr.precio > 0
        """), {"keys": keys}).fetchall()

    # Build per-product, per-pharmacy price map
    product_prices = {}
    all_farmacias = set()
    for r in prices:
        k = r.nombre_normalizado
        if k not in product_prices:
            product_prices[k] = {}
        if r.farmacia not in product_prices[k] or r.precio < product_prices[k][r.farmacia]:
            product_prices[k][r.farmacia] = float(r.precio)
        all_farmacias.add(r.farmacia)

    # Calculate totals per farmacia
    totales = {}
    for farm in all_farmacias:
        total = 0
        complete = True
        for p in productos:
            price = product_prices.get(p.nombre_normalizado, {}).get(farm)
            if price:
                total += price
            else:
                complete = False
        if complete and total > 0:
            totales[farm] = round(total, 2)

    farmacia_optima = min(totales, key=totales.get) if totales else None
    ahorro = round(max(totales.values()) - min(totales.values()), 2) if len(totales) >= 2 else 0

    return jsonify({
        "id": lista.id, "nombre": lista.nombre,
        "productos": [{"nombre_normalizado": p.nombre_normalizado, "nombre_display": p.nombre_display,
                       "precios": product_prices.get(p.nombre_normalizado, {})} for p in productos],
        "totales": totales,
        "farmacia_optima": farmacia_optima,
        "ahorro": ahorro
    })


# ============================================================
# API — Side-by-Side Comparison
# ============================================================
@app.route('/api/comparar', methods=['POST'])
def comparar_productos():
    data = request.get_json()
    keys = data.get('productos', [])
    if not keys or len(keys) < 2 or len(keys) > 5:
        return jsonify({"error": "Selecciona entre 2 y 5 productos"}), 400
    with engine.connect() as con:
        rows = con.execute(text("""
            SELECT p.nombre_normalizado, p.nombre, p.farmacia, p.url, p.ean, pr.precio, pr.en_stock
            FROM productos p
            JOIN (SELECT DISTINCT ON (producto_id) producto_id, precio, en_stock FROM precios ORDER BY producto_id, fecha_captura DESC) pr
            ON p.id = pr.producto_id
            WHERE p.nombre_normalizado = ANY(:keys)
        """), {"keys": keys}).fetchall()
    result = {}
    for r in rows:
        k = r.nombre_normalizado
        if k not in result:
            result[k] = {"nombre": r.nombre, "ean": r.ean, "farmacias": {}}
        result[k]["farmacias"][r.farmacia] = {
            "precio": float(r.precio) if r.precio else None,
            "en_stock": bool(r.en_stock), "url": r.url
        }
    return jsonify(result)


# ============================================================
# API — Similar Products
# ============================================================
@app.route('/api/similares')
def get_similares():
    key = request.args.get('nombre_normalizado', '').strip()
    if not key:
        return jsonify({"error": "nombre_normalizado requerido"}), 400
    # Get first 3 significant words from the normalized key for matching
    words = [w for w in key.split() if len(w) > 3][:3]
    if not words:
        words = key.split()[:2]
    pattern = '%'.join(words)
    with engine.connect() as con:
        rows = con.execute(text("""
            WITH latest_prices AS (
                SELECT DISTINCT ON (producto_id) producto_id, precio
                FROM precios WHERE precio > 0 ORDER BY producto_id, fecha_captura DESC
            )
            SELECT DISTINCT ON (p.nombre_normalizado)
                p.nombre_normalizado, p.nombre, pr.precio, p.farmacia
            FROM productos p
            JOIN latest_prices pr ON p.id = pr.producto_id
            WHERE p.nombre_normalizado LIKE :pattern
              AND p.nombre_normalizado != :key
            ORDER BY p.nombre_normalizado, pr.precio ASC
            LIMIT 10
        """), {"pattern": f"%{pattern}%", "key": key}).fetchall()
    return jsonify([{
        "nombre_normalizado": r.nombre_normalizado, "nombre": r.nombre,
        "precio": float(r.precio), "farmacia": r.farmacia
    } for r in rows])


# ============================================================
# API — User Dashboard
# ============================================================
@app.route('/api/dashboard')
@user_required
def get_dashboard():
    uid = session['user_id']
    with engine.connect() as con:
        fav_count = con.execute(text("SELECT COUNT(*) FROM favoritos WHERE usuario_id = :uid"), {"uid": uid}).scalar()
        hist_count = con.execute(text("SELECT COUNT(*) FROM historial_busquedas WHERE usuario_id = :uid"), {"uid": uid}).scalar()
        alert_count = con.execute(text("SELECT COUNT(*) FROM alertas_precio WHERE usuario_id = :uid AND activa = true"), {"uid": uid}).scalar()
        lista_count = con.execute(text("SELECT COUNT(*) FROM listas WHERE usuario_id = :uid"), {"uid": uid}).scalar()
        user = con.execute(text("SELECT plan, fecha_registro FROM usuarios WHERE id = :uid"), {"uid": uid}).fetchone()
    return jsonify({
        "favoritos": fav_count, "historial": hist_count,
        "alertas": alert_count, "listas": lista_count,
        "plan": user.plan, "miembro_desde": user.fecha_registro.isoformat() if user.fecha_registro else None
    })


# ============================================================
# API — Product Catalog (Plan Premium 60 EUR/mes)
# ============================================================
@app.route('/api/catalogo')
@user_required
def get_catalogo():
    """Get pharmacy's product catalog with competition prices."""
    with engine.connect() as con:
        user = con.execute(text("SELECT plan FROM usuarios WHERE id = :uid"), {"uid": session['user_id']}).fetchone()
        if user.plan != 'premium':
            return jsonify({"error": "Esta funcion requiere el Plan Premium", "plan_required": "farmacia"}), 403

        rows = con.execute(text("""
            SELECT c.id, c.nombre_producto, c.nombre_normalizado, c.precio_propio,
                   c.precio_competencia_min, c.precio_competencia_max,
                   c.farmacias_competencia, c.ultimo_cambio, c.fecha_creacion
            FROM catalogo_farmacia c WHERE c.usuario_id = :uid
            ORDER BY c.nombre_producto
        """), {"uid": session['user_id']}).fetchall()
    return jsonify([{
        "id": r.id, "nombre": r.nombre_producto, "nombre_normalizado": r.nombre_normalizado,
        "precio_propio": float(r.precio_propio) if r.precio_propio else None,
        "precio_min": float(r.precio_competencia_min) if r.precio_competencia_min else None,
        "precio_max": float(r.precio_competencia_max) if r.precio_competencia_max else None,
        "farmacias": r.farmacias_competencia,
        "ultimo_cambio": r.ultimo_cambio.isoformat() if r.ultimo_cambio else None,
        "fecha": r.fecha_creacion.isoformat() if r.fecha_creacion else None
    } for r in rows])


@app.route('/api/catalogo', methods=['POST'])
@user_required
def add_catalogo():
    """Add product to pharmacy catalog."""
    with engine.connect() as con:
        user = con.execute(text("SELECT plan FROM usuarios WHERE id = :uid"), {"uid": session['user_id']}).fetchone()
        if user.plan != 'premium':
            return jsonify({"error": "Requiere Plan Premium"}), 403

    data = request.get_json()
    nombre = (data.get('nombre') or '').strip()
    precio_propio = data.get('precio_propio')
    if not nombre:
        return jsonify({"error": "Nombre requerido"}), 400

    # Normalize name for matching
    nombre_norm = nombre.lower().replace(' ', '')

    with engine.connect() as con:
        # Check if product exists in our DB and get competition prices
        comp = con.execute(text("""
            WITH latest AS (
                SELECT DISTINCT ON (p.id) p.farmacia, pr.precio
                FROM productos p JOIN precios pr ON p.id = pr.producto_id
                WHERE p.nombre_normalizado LIKE '%' || normalize_product_name(:name) || '%'
                ORDER BY p.id, pr.fecha_captura DESC
            )
            SELECT MIN(precio) as min_price, MAX(precio) as max_price,
                   STRING_AGG(DISTINCT farmacia, ', ') as farmacias
            FROM latest WHERE precio > 0
        """), {"name": nombre}).fetchone()

        con.execute(text("""
            INSERT INTO catalogo_farmacia (usuario_id, nombre_producto, nombre_normalizado,
                                          precio_propio, precio_competencia_min, precio_competencia_max,
                                          farmacias_competencia)
            VALUES (:uid, :nombre, :norm, :precio, :min, :max, :farms)
            ON CONFLICT (usuario_id, nombre_normalizado) DO UPDATE
            SET precio_propio = :precio, precio_competencia_min = :min,
                precio_competencia_max = :max, farmacias_competencia = :farms
        """), {
            "uid": session['user_id'], "nombre": nombre, "norm": nombre_norm,
            "precio": precio_propio,
            "min": float(comp.min_price) if comp and comp.min_price else None,
            "max": float(comp.max_price) if comp and comp.max_price else None,
            "farms": comp.farmacias if comp else None
        })
        con.commit()

    return jsonify({"message": "Producto añadido al catalogo"}), 201


@app.route('/api/catalogo/<int:item_id>', methods=['DELETE'])
@user_required
def delete_catalogo(item_id):
    with engine.connect() as con:
        con.execute(text("DELETE FROM catalogo_farmacia WHERE id = :id AND usuario_id = :uid"),
                    {"id": item_id, "uid": session['user_id']})
        con.commit()
    return jsonify({"message": "Eliminado del catalogo"})


@app.route('/api/catalogo/importar-excel', methods=['POST'])
@user_required
def importar_catalogo_excel():
    """Import products from Excel file (columns: nombre, ean, precio)."""
    with engine.connect() as con:
        user = con.execute(text("SELECT plan FROM usuarios WHERE id = :uid"), {"uid": session['user_id']}).fetchone()
        if user.plan != 'premium':
            return jsonify({"error": "Requiere Plan Premium"}), 403

    if 'file' not in request.files:
        return jsonify({"error": "No se ha enviado ningun archivo"}), 400

    file = request.files['file']
    if not file.filename.endswith(('.xlsx', '.xls', '.csv')):
        return jsonify({"error": "Formato no soportado. Usa .xlsx o .csv"}), 400

    try:
        import pandas as pd
        import io

        if file.filename.endswith('.csv'):
            df = pd.read_csv(io.BytesIO(file.read()), encoding='utf-8')
        else:
            df = pd.read_excel(io.BytesIO(file.read()))

        # Normalize column names
        df.columns = [c.strip().lower() for c in df.columns]

        # Find required columns
        nombre_col = next((c for c in df.columns if c in ['nombre', 'producto', 'name', 'descripcion']), None)
        if not nombre_col:
            return jsonify({"error": "No se encontro columna 'nombre' o 'producto' en el Excel"}), 400

        ean_col = next((c for c in df.columns if c in ['ean', 'ean13', 'codigo', 'barcode', 'gtin']), None)
        precio_col = next((c for c in df.columns if c in ['precio', 'pvp', 'price', 'precio_venta']), None)

        added = 0
        found = 0
        pending = 0

        with engine.connect() as con:
            for _, row in df.iterrows():
                nombre = str(row[nombre_col]).strip() if pd.notna(row[nombre_col]) else None
                if not nombre or nombre == 'nan':
                    continue

                ean = str(row[ean_col]).strip() if ean_col and pd.notna(row.get(ean_col)) else None
                if ean and ean == 'nan':
                    ean = None
                # Clean EAN: remove decimals if read as float
                if ean and '.' in ean:
                    ean = ean.split('.')[0]

                precio_propio = None
                if precio_col and pd.notna(row.get(precio_col)):
                    try:
                        precio_propio = float(str(row[precio_col]).replace(',', '.').replace('€', '').strip())
                    except:
                        pass

                nombre_norm = nombre.lower().replace(' ', '')

                # Try to find in our DB by EAN first, then by name
                comp = None
                if ean and len(ean) >= 7:
                    comp = con.execute(text("""
                        WITH latest AS (
                            SELECT DISTINCT ON (p.id) p.farmacia, pr.precio
                            FROM productos p JOIN precios pr ON p.id = pr.producto_id
                            WHERE p.ean = :ean
                            ORDER BY p.id, pr.fecha_captura DESC
                        )
                        SELECT MIN(precio) as min_price, MAX(precio) as max_price,
                               STRING_AGG(DISTINCT farmacia, ', ') as farmacias
                        FROM latest WHERE precio > 0
                    """), {"ean": ean}).fetchone()

                if not comp or not comp.min_price:
                    # Try by name
                    comp = con.execute(text("""
                        WITH latest AS (
                            SELECT DISTINCT ON (p.id) p.farmacia, pr.precio
                            FROM productos p JOIN precios pr ON p.id = pr.producto_id
                            WHERE p.nombre_normalizado LIKE '%' || normalize_product_name(:name) || '%'
                            ORDER BY p.id, pr.fecha_captura DESC
                        )
                        SELECT MIN(precio) as min_price, MAX(precio) as max_price,
                               STRING_AGG(DISTINCT farmacia, ', ') as farmacias
                        FROM latest WHERE precio > 0
                    """), {"name": nombre}).fetchone()

                status = 'encontrado' if (comp and comp.min_price) else 'pendiente'
                if status == 'encontrado':
                    found += 1
                else:
                    pending += 1

                con.execute(text("""
                    INSERT INTO catalogo_farmacia (usuario_id, nombre_producto, nombre_normalizado,
                                                  precio_propio, precio_competencia_min, precio_competencia_max,
                                                  farmacias_competencia)
                    VALUES (:uid, :nombre, :norm, :precio, :min, :max, :farms)
                    ON CONFLICT (usuario_id, nombre_normalizado) DO UPDATE
                    SET precio_propio = COALESCE(:precio, catalogo_farmacia.precio_propio),
                        precio_competencia_min = COALESCE(:min, catalogo_farmacia.precio_competencia_min),
                        precio_competencia_max = COALESCE(:max, catalogo_farmacia.precio_competencia_max),
                        farmacias_competencia = COALESCE(:farms, catalogo_farmacia.farmacias_competencia)
                """), {
                    "uid": session['user_id'], "nombre": nombre, "norm": nombre_norm,
                    "precio": precio_propio,
                    "min": float(comp.min_price) if comp and comp.min_price else None,
                    "max": float(comp.max_price) if comp and comp.max_price else None,
                    "farms": comp.farmacias if comp and comp.farmacias else None
                })
                added += 1

            con.commit()

        return jsonify({
            "message": f"Importacion completada: {added} productos procesados",
            "total": added,
            "encontrados": found,
            "pendientes": pending
        })

    except Exception as e:
        return jsonify({"error": f"Error procesando archivo: {str(e)[:200]}"}), 500


@app.route('/api/catalogo/actualizar-precios', methods=['POST'])
@user_required
def actualizar_catalogo_precios():
    """Re-check competition prices for all catalog products."""
    with engine.connect() as con:
        user = con.execute(text("SELECT plan FROM usuarios WHERE id = :uid"), {"uid": session['user_id']}).fetchone()
        if user.plan != 'premium':
            return jsonify({"error": "Requiere Plan Premium"}), 403

        items = con.execute(text("""
            SELECT id, nombre_producto, nombre_normalizado, precio_competencia_min
            FROM catalogo_farmacia WHERE usuario_id = :uid
        """), {"uid": session['user_id']}).fetchall()

        cambios = 0
        for item in items:
            comp = con.execute(text("""
                WITH latest AS (
                    SELECT DISTINCT ON (p.id) p.farmacia, pr.precio
                    FROM productos p JOIN precios pr ON p.id = pr.producto_id
                    WHERE p.nombre_normalizado LIKE '%' || normalize_product_name(:name) || '%'
                    ORDER BY p.id, pr.fecha_captura DESC
                )
                SELECT MIN(precio) as min_price, MAX(precio) as max_price,
                       STRING_AGG(DISTINCT farmacia, ', ') as farmacias
                FROM latest WHERE precio > 0
            """), {"name": item.nombre_producto}).fetchone()

            if comp and comp.min_price:
                new_min = float(comp.min_price)
                old_min = float(item.precio_competencia_min) if item.precio_competencia_min else None

                # Detect price change
                if old_min and abs(new_min - old_min) >= 0.01:
                    con.execute(text("""
                        INSERT INTO cambios_precio (catalogo_id, usuario_id, farmacia_competencia,
                                                   precio_anterior, precio_nuevo)
                        VALUES (:cid, :uid, :farm, :old, :new)
                    """), {"cid": item.id, "uid": session['user_id'],
                           "farm": comp.farmacias, "old": old_min, "new": new_min})
                    cambios += 1

                con.execute(text("""
                    UPDATE catalogo_farmacia SET precio_competencia_min = :min,
                           precio_competencia_max = :max, farmacias_competencia = :farms,
                           ultimo_cambio = CASE WHEN :changed THEN CURRENT_TIMESTAMP ELSE ultimo_cambio END
                    WHERE id = :id
                """), {"min": new_min, "max": float(comp.max_price) if comp.max_price else None,
                       "farms": comp.farmacias, "changed": old_min and abs(new_min - (old_min or 0)) >= 0.01,
                       "id": item.id})

        con.commit()

    return jsonify({"message": f"Precios actualizados. {cambios} cambios detectados.", "cambios": cambios})


@app.route('/api/catalogo/cambios')
@user_required
def get_cambios_precio():
    """Get recent price changes for catalog products."""
    with engine.connect() as con:
        rows = con.execute(text("""
            SELECT cp.id, cf.nombre_producto, cp.farmacia_competencia,
                   cp.precio_anterior, cp.precio_nuevo, cp.fecha_deteccion
            FROM cambios_precio cp
            JOIN catalogo_farmacia cf ON cp.catalogo_id = cf.id
            WHERE cp.usuario_id = :uid
            ORDER BY cp.fecha_deteccion DESC LIMIT 50
        """), {"uid": session['user_id']}).fetchall()
    return jsonify([{
        "nombre": r.nombre_producto, "farmacia": r.farmacia_competencia,
        "precio_anterior": float(r.precio_anterior), "precio_nuevo": float(r.precio_nuevo),
        "cambio": round(float(r.precio_nuevo) - float(r.precio_anterior), 2),
        "fecha": r.fecha_deteccion.isoformat() if r.fecha_deteccion else None
    } for r in rows])


# ============================================================
# API — Scraper Monitoring
# ============================================================
@app.route('/api/scrapers/status')
@admin_required
def get_scrapers_status():
    """Get latest run status for each scraper + product counts from DB."""
    with engine.connect() as con:
        # Latest run per farmacia
        runs = con.execute(text("""
            SELECT DISTINCT ON (farmacia) farmacia, estado, productos_total,
                   productos_nuevos, productos_actualizados, errores, mensaje,
                   inicio, fin, duracion_seg
            FROM scraper_runs ORDER BY farmacia, inicio DESC
        """)).fetchall()

        # Current product counts per farmacia
        counts = con.execute(text("""
            SELECT farmacia, COUNT(*) as total FROM productos GROUP BY farmacia ORDER BY farmacia
        """)).fetchall()

        # Total price records
        total_precios = con.execute(text("SELECT COUNT(*) FROM precios")).scalar()

    count_map = {r.farmacia: r.total for r in counts}
    total_productos = sum(count_map.values())

    run_list = [{
        "farmacia": r.farmacia, "estado": r.estado,
        "productos_total": r.productos_total, "productos_nuevos": r.productos_nuevos,
        "productos_actualizados": r.productos_actualizados, "errores": r.errores,
        "mensaje": r.mensaje,
        "inicio": r.inicio.isoformat() if r.inicio else None,
        "fin": r.fin.isoformat() if r.fin else None,
        "duracion_seg": r.duracion_seg
    } for r in runs]

    farmacias_info = [
        {"nombre": "DosFarma", "key": "DosFarma", "metodo": "Algolia API", "productos": count_map.get("DosFarma", 0),
         "levels": {1: True, 2: True, 3: True}},
        {"nombre": "Atida", "key": "Atida", "metodo": "Algolia API", "productos": count_map.get("Atida", 0),
         "levels": {1: True, 2: True, 3: True}},
        {"nombre": "PromoFarma", "key": "PromoFarma", "metodo": "Sitemap + Playwright", "productos": count_map.get("PromoFarma", 0),
         "levels": {1: False, 2: True, 3: True}},
        {"nombre": "FarmaciasVazquez", "key": "FarmaciasVazquez", "metodo": "Doofinder API", "productos": count_map.get("FarmaciasVazquez", 0),
         "levels": {1: True, 2: True, 3: True}},
        {"nombre": "FarmaciasDirect", "key": "FarmaciasDirect", "metodo": "Empathy API", "productos": count_map.get("FarmaciasDirect", 0),
         "levels": {1: True, 2: True, 3: True}},
        {"nombre": "Farmacoslada", "key": "Farmacoslada", "metodo": "Sitemap + aiohttp", "productos": count_map.get("Farmacoslada", 0),
         "levels": {1: False, 2: True, 3: True}},
        {"nombre": "Okfarma", "key": "Okfarma", "metodo": "Sitemap + Playwright", "productos": count_map.get("Okfarma", 0),
         "levels": {1: False, 2: True, 3: True}},
        {"nombre": "FarmaciaBarata", "key": "FarmaciaBarata", "metodo": "Sitemap + Playwright", "productos": count_map.get("FarmaciaBarata", 0),
         "levels": {1: False, 2: True, 3: True}},
    ]

    return jsonify({
        "farmacias": farmacias_info,
        "runs": run_list,
        "total_productos": total_productos,
        "total_precios": total_precios
    })


import subprocess
import threading

# Active scraper processes
active_scrapers = {}

@app.route('/api/scrapers/run', methods=['POST'])
@admin_required
def run_scraper():
    """Start a scraper in background."""
    data = request.get_json()
    farmacia = (data.get('farmacia') or '').strip()

    scraper_levels = {
        'DosFarma':        {1: 'scrapers/dosfarma/level1_api.py', 2: 'scrapers/dosfarma/level2_http.py', 3: 'scrapers/dosfarma/level3_playwright.py'},
        'Atida':           {1: 'scrapers/atida/level1_api.py', 2: 'scrapers/atida/level2_http.py', 3: 'scrapers/atida/level3_playwright.py'},
        'PromoFarma':      {2: 'scrapers/promofarma/level2_http.py', 3: 'scrapers/promofarma/level3_playwright.py'},
        'FarmaciasVazquez': {1: 'scrapers/vazquez/level1_api.py', 2: 'scrapers/vazquez/level2_http.py', 3: 'scrapers/vazquez/level3_playwright.py'},
        'FarmaciasDirect':  {1: 'scrapers/farmaciasdirect/level1_api.py', 2: 'scrapers/farmaciasdirect/level2_http.py', 3: 'scrapers/farmaciasdirect/level3_playwright.py'},
        'Farmacoslada':     {2: 'scrapers/farmacoslada/level2_http.py', 3: 'scrapers/farmacoslada/level3_playwright.py'},
        'Okfarma':          {2: 'scrapers/okfarma/level2_http.py', 3: 'scrapers/okfarma/level3_playwright.py'},
        'FarmaciaBarata':   {2: 'scrapers/farmaciabarata/level2_http.py', 3: 'scrapers/farmaciabarata/level3_playwright.py'},
    }

    if farmacia not in scraper_levels:
        return jsonify({"error": f"Farmacia '{farmacia}' no reconocida"}), 400

    if farmacia in active_scrapers and active_scrapers[farmacia].poll() is None:
        return jsonify({"error": f"{farmacia} ya esta ejecutandose"}), 409

    limit = data.get('limit', 0)
    level = data.get('level')  # None = auto fallback
    levels_available = scraper_levels[farmacia]

    # Pick the script for the requested level, or auto-select
    if level:
        if level not in levels_available:
            return jsonify({"error": f"Nivel {level} no disponible para {farmacia}"}), 400
        script = levels_available[level]
    else:
        # Auto: try level 1, then 2, then 3
        script = levels_available.get(1, levels_available.get(2, levels_available.get(3)))

    cmd = [sys.executable, script]
    if limit and int(limit) > 0:
        cmd += ['--limit', str(limit)]

    # Record run start in DB
    with engine.connect() as con:
        result = con.execute(text("""
            INSERT INTO scraper_runs (farmacia, estado) VALUES (:f, 'running') RETURNING id
        """), {"f": farmacia})
        con.commit()
        run_id = result.fetchone()[0]

    def run_and_record(cmd, farmacia, run_id):
        try:
            proc = subprocess.Popen(cmd, cwd=os.path.dirname(os.path.abspath(__file__)),
                                     stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                     bufsize=1)
            active_scrapers[farmacia] = proc
            output_lines = []
            import select
            import time as _time

            last_update = _time.time()
            while proc.poll() is None:
                # Read available output
                try:
                    line = proc.stdout.readline()
                    if line:
                        output_lines.append(line.decode('utf-8', errors='replace').rstrip())
                        # Keep last 50 lines
                        if len(output_lines) > 50:
                            output_lines = output_lines[-50:]
                        # Update DB every 5 seconds
                        if _time.time() - last_update > 5:
                            last_update = _time.time()
                            msg = '\n'.join(output_lines[-30:])
                            try:
                                with engine.connect() as con2:
                                    con2.execute(text("UPDATE scraper_runs SET mensaje = :msg WHERE id = :id"),
                                                {"msg": msg, "id": run_id})
                                    con2.commit()
                            except: pass
                except: break

            # Read remaining output
            remaining = proc.stdout.read()
            if remaining:
                output_lines.extend(remaining.decode('utf-8', errors='replace').rstrip().split('\n'))

            estado = 'success' if proc.returncode == 0 else 'error'
            msg = '\n'.join(output_lines[-40:])
        except subprocess.TimeoutExpired:
            proc.kill()
            estado = 'error'
            msg = 'Timeout (>2h)'
        except Exception as e:
            estado = 'error'
            msg = str(e)[:500]
        finally:
            active_scrapers.pop(farmacia, None)

        # Update run record
        with engine.connect() as con:
            con.execute(text("""
                UPDATE scraper_runs SET estado = :estado, mensaje = :msg,
                       fin = CURRENT_TIMESTAMP,
                       duracion_seg = EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - inicio))::int
                WHERE id = :id
            """), {"estado": estado, "msg": msg, "id": run_id})
            con.commit()

    thread = threading.Thread(target=run_and_record, args=(cmd, farmacia, run_id), daemon=True)
    thread.start()

    return jsonify({"message": f"Scraper {farmacia} iniciado", "run_id": run_id})


@app.route('/api/scrapers/logs/<int:run_id>')
@admin_required
def get_scraper_logs(run_id):
    """Get logs/output for a specific scraper run."""
    with engine.connect() as con:
        row = con.execute(text("SELECT farmacia, estado, mensaje, productos_total, inicio, fin, duracion_seg FROM scraper_runs WHERE id = :id"),
                         {"id": run_id}).fetchone()
    if not row:
        return jsonify({"error": "Run not found"}), 404
    return jsonify({
        "farmacia": row.farmacia, "estado": row.estado,
        "mensaje": row.mensaje or "", "productos_total": row.productos_total,
        "inicio": row.inicio.isoformat() if row.inicio else None,
        "fin": row.fin.isoformat() if row.fin else None,
        "duracion_seg": row.duracion_seg
    })


@app.route('/api/scrapers/stop', methods=['POST'])
@admin_required
def stop_scraper():
    """Stop a running scraper."""
    data = request.get_json()
    farmacia = (data.get('farmacia') or '').strip()
    if farmacia in active_scrapers and active_scrapers[farmacia].poll() is None:
        active_scrapers[farmacia].kill()
        active_scrapers.pop(farmacia, None)
        return jsonify({"message": f"Scraper {farmacia} detenido"})
    return jsonify({"error": "No hay scraper activo para esta farmacia"}), 404


# ============================================================
# API — Pharmacy locations
# ============================================================
@app.route('/api/farmacias/ubicaciones', methods=['GET'])
def get_ubicaciones():
    farmacia_filter = request.args.get('farmacia', '').strip()
    if farmacia_filter:
        query = text("""
            SELECT id, farmacia, nombre_tienda, direccion, latitud, longitud, telefono, horario
            FROM farmacia_ubicaciones WHERE activa = true AND LOWER(farmacia) = :farmacia
            ORDER BY farmacia, nombre_tienda
        """)
        params = {"farmacia": farmacia_filter.lower()}
    else:
        query = text("""
            SELECT id, farmacia, nombre_tienda, direccion, latitud, longitud, telefono, horario
            FROM farmacia_ubicaciones WHERE activa = true ORDER BY farmacia, nombre_tienda
        """)
        params = {}

    with engine.connect() as con:
        rows = con.execute(query, params).fetchall()

    return jsonify([{
        "id": r.id, "farmacia": r.farmacia, "nombre_tienda": r.nombre_tienda,
        "direccion": r.direccion, "latitud": float(r.latitud), "longitud": float(r.longitud),
        "telefono": r.telefono, "horario": r.horario
    } for r in rows])


@app.route('/api/farmacias/ubicaciones', methods=['POST'])
@farmacia_required
def add_ubicacion():
    data = request.get_json()
    for field in ['nombre_tienda', 'direccion', 'latitud', 'longitud']:
        if not data.get(field):
            return jsonify({"error": f"Campo '{field}' es obligatorio"}), 400

    query = text("""
        INSERT INTO farmacia_ubicaciones (farmacia, nombre_tienda, direccion, latitud, longitud, telefono, horario, activa)
        VALUES (:farmacia, :nombre, :dir, :lat, :lng, :tel, :horario, true) RETURNING id
    """)
    with engine.connect() as con:
        result = con.execute(query, {
            "farmacia": session['farmacia_nombre'], "nombre": data['nombre_tienda'],
            "dir": data['direccion'], "lat": data['latitud'], "lng": data['longitud'],
            "tel": data.get('telefono', ''), "horario": data.get('horario', '')
        })
        con.commit()
        new_id = result.fetchone()[0]
    return jsonify({"id": new_id, "message": "Ubicacion anadida"}), 201


@app.route('/api/farmacias/ubicaciones/<int:ubicacion_id>', methods=['DELETE'])
@farmacia_required
def delete_ubicacion(ubicacion_id):
    query = text("""
        DELETE FROM farmacia_ubicaciones
        WHERE id = :id AND LOWER(farmacia) = LOWER(:farmacia) RETURNING id
    """)
    with engine.connect() as con:
        result = con.execute(query, {"id": ubicacion_id, "farmacia": session['farmacia_nombre']})
        con.commit()
        deleted = result.fetchone()
    if not deleted:
        return jsonify({"error": "No encontrada o sin permiso"}), 404
    return jsonify({"message": "Eliminada"})


@app.route('/api/farmacias/mis-ubicaciones')
@farmacia_required
def mis_ubicaciones():
    query = text("""
        SELECT id, nombre_tienda, direccion, latitud, longitud, telefono, horario
        FROM farmacia_ubicaciones WHERE LOWER(farmacia) = LOWER(:farmacia) AND activa = true
        ORDER BY nombre_tienda
    """)
    with engine.connect() as con:
        rows = con.execute(query, {"farmacia": session['farmacia_nombre']}).fetchall()
    return jsonify([{
        "id": r.id, "nombre_tienda": r.nombre_tienda, "direccion": r.direccion,
        "latitud": float(r.latitud), "longitud": float(r.longitud),
        "telefono": r.telefono, "horario": r.horario
    } for r in rows])


# ============================================================
# API — Error reporting
# ============================================================
from utils.email_utils import send_email

@app.route('/api/report_error', methods=['POST'])
def report_error():
    data = request.json
    if not data or 'producto' not in data or 'mensaje' not in data:
        return jsonify({"error": "Faltan datos"}), 400

    producto = data.get('producto', 'Desconocido')
    farmacias = data.get('farmacias', [])
    mensaje = data.get('mensaje', '')
    farmacias_str = ", ".join(farmacias) if farmacias else "Ninguna"

    exito = send_email(
        subject=f"FarmaSearch: Error reportado en '{producto[:30]}...'",
        body=f"""
        <h2>Alerta de Agrupacion Incorrecta</h2>
        <p>Producto: <b>{producto}</b></p>
        <p>Farmacias: {farmacias_str}</p>
        <blockquote style="background:#f9f9f9;border-left:4px solid #f57c00;padding:10px">{mensaje}</blockquote>
        """
    )
    if exito:
        return jsonify({"message": "Reporte enviado"})
    return jsonify({"error": "No se pudo enviar el email"}), 500


if __name__ == '__main__':
    print("\n[*] FarmaSearch iniciado en http://localhost:5000\n")
    port = int(os.getenv('PORT', 5000))
    debug = os.getenv('FLASK_ENV') != 'production'
    app.run(debug=debug, host='0.0.0.0', port=port)
