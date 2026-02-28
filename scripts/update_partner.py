"""Update partner pharmacy in DB."""
from db_models import get_engine
from sqlalchemy import text

e = get_engine()
con = e.connect()

# Find the pharmacy
rows = con.execute(text(
    "SELECT id, nombre_tienda, direccion, telefono FROM farmacia_ubicaciones "
    "WHERE telefono LIKE :tel"
), {"tel": "%915396138%"}).fetchall()

if rows:
    for r in rows:
        print(f"Found: id={r.id}, name={r.nombre_tienda}, addr={r.direccion}, tel={r.telefono}")
    
    # Mark it as partner
    partner_id = rows[0].id
    con.execute(text(
        "UPDATE farmacia_ubicaciones SET farmacia = 'FarmaSearch' WHERE id = :id"
    ), {"id": partner_id})
    con.commit()
    print(f"\nUpdated id={partner_id} to farmacia='FarmaSearch'")
else:
    print("Not found by phone, searching by address...")
    rows = con.execute(text(
        "SELECT id, nombre_tienda, direccion, telefono FROM farmacia_ubicaciones "
        "WHERE direccion LIKE :addr"
    ), {"addr": "%Bronce%"}).fetchall()
    for r in rows:
        print(f"Found: id={r.id}, name={r.nombre_tienda}, addr={r.direccion}, tel={r.telefono}")
    
    if rows:
        partner_id = rows[0].id
        con.execute(text(
            "UPDATE farmacia_ubicaciones SET farmacia = 'FarmaSearch' WHERE id = :id"
        ), {"id": partner_id})
        con.commit()
        print(f"\nUpdated id={partner_id} to farmacia='FarmaSearch'")
    else:
        print("Pharmacy not found! Adding it manually...")
        con.execute(text(
            "INSERT INTO farmacia_ubicaciones (farmacia, nombre_tienda, direccion, latitud, longitud, telefono, horario, activa) "
            "VALUES ('FarmaSearch', 'Farmacia', 'Calle del Bronce 33, 28045, Madrid', 40.3922, -3.6946, '+34 915396138', '', true)"
        ))
        con.commit()
        print("Inserted manually as FarmaSearch partner")

# Verify
count = con.execute(text(
    "SELECT COUNT(*) FROM farmacia_ubicaciones WHERE farmacia = 'FarmaSearch'"
)).scalar()
print(f"\nTotal FarmaSearch partners: {count}")

con.close()
