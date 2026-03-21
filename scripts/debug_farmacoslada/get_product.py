import subprocess
import time
import webbrowser
import socket
from sqlalchemy import text
from db_models import get_engine

engine = get_engine()
with engine.connect() as con:
    res = con.execute(text("SELECT nombre FROM productos WHERE farmacia = 'Farmacoslada' AND TRIM(nombre) != '' LIMIT 1")).fetchone()
    if res:
        print("PRODUCTO_SUGERIDO:", res[0])
    
def is_port_open():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('127.0.0.1', 5000)) == 0

if not is_port_open():
    print("Arrancando web_app.py...")
    import os
    python_bin = os.path.join(os.getcwd(), 'venv', 'Scripts', 'python.exe')
    if not os.path.exists(python_bin):
        python_bin = 'python'
    subprocess.Popen([python_bin, 'web_app.py'])
    for _ in range(10):
        if is_port_open():
            break
        time.sleep(0.5)

print("Abriendo navegador...")
webbrowser.open("http://localhost:5000")
