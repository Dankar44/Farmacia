from run_all import send_email, backup_database
import logging
import os
from datetime import datetime

# Set up simple logging for test
os.makedirs("logs", exist_ok=True)
logging.basicConfig(level=logging.INFO)

print("1. Testing database backup...")
backup_path, success = backup_database()
if success:
    print(f"[OK] Backup successful! Saved to: {backup_path}")
    backup_msg = f"<p>✅ <strong>Backup DB:</strong> Éxito. Guardado en <code>{backup_path}</code></p>"
else:
    print("[ERROR] Backup failed. Check email or logs for details.")
    backup_msg = f"<p>❌ <strong>Backup DB:</strong> Falló. Error: {backup_path}</p>"

print("\n2. Testing email sending...")
test_body = f"""
<h2>Prueba de Conexión - FarmaSearch Scraping</h2>
<p>Este es un correo de prueba para verificar que la automatización de FarmaSearch puede enviar correos correctamente.</p>
{backup_msg}
<p><strong>Hora de la prueba:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
<hr>
<p><em>Si estás leyendo esto, el sistema de automatización está listo para programarse en Windows.</em></p>
"""

try:
    send_email("🧪 Prueba Sistema Automatizado FarmaSearch", test_body)
    print("[OK] Email sent successfully! Check daniel.karimi@alumnos.upm.es")
except Exception as e:
    print(f"[ERROR] Email failed: {e}")
