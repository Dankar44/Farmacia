import os
import subprocess
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import sys
import logging
from dotenv import load_dotenv

load_dotenv()

# Set up logging
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename=f"logs/run_all_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Email config - load from .env or hardcode for now
EMAIL_SENDER = os.getenv("SMTP_USER", "danisuperk@gmail.com") 
EMAIL_PASSWORD = os.getenv("SMTP_PASS", "urxvfyzsjkrfnlmw")
EMAIL_RECEIVER = "daniel.karimi@alumnos.upm.es"

def send_email(subject, body):
    if EMAIL_SENDER == "your-email@gmail.com":
        logging.warning("Email credentials not configured. Skipping email sent.")
        print("⚠️ Configura SMTP_USER y SMTP_PASS en run_all.py para enviar correos.")
        return

    msg = MIMEMultipart()
    msg['From'] = EMAIL_SENDER
    msg['To'] = EMAIL_RECEIVER
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'html'))

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        text = msg.as_string()
        server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, text)
        server.quit()
        logging.info("Email sent successfully")
    except Exception as e:
        logging.error(f"Failed to send email: {e}")

def backup_database():
    logging.info("Starting database backup...")
    os.makedirs("backups", exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_file = os.path.join(os.getcwd(), "backups", f"farmasearch_db_backup_{timestamp}.sql")
    
    db_pass = os.getenv("DB_PASSWORD", "ali8Reza@")
    db_user = os.getenv("DB_USER", "postgres")
    db_name = os.getenv("DB_NAME", "farmacia_scraper_db")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    
    os.environ["PGPASSWORD"] = db_pass
    
    pg_dump_path = "pg_dump"
    if os.name == 'nt':
        # Buscamos pg_dump en rutas comunes de Windows
        common_paths = [
            r"C:\Program Files\PostgreSQL\17\bin\pg_dump.exe",
            r"C:\Program Files\PostgreSQL\16\bin\pg_dump.exe",
            r"C:\Program Files\PostgreSQL\15\bin\pg_dump.exe",
            r"C:\Program Files\PostgreSQL\14\bin\pg_dump.exe",
            r"C:\Program Files\PostgreSQL\13\bin\pg_dump.exe",
            r"C:\Program Files\PostgreSQL\12\bin\pg_dump.exe",
        ]
        for path in common_paths:
            if os.path.exists(path):
                pg_dump_path = path
                break

    cmd = [
        pg_dump_path, 
        "-U", db_user, 
        "-h", db_host, 
        "-p", db_port,
        "-F", "c", # Custom format (compressed)
        "-f", backup_file, 
        db_name
    ]
    
    try:
        subprocess.run(cmd, check=True, capture_output=True)
        logging.info(f"Backup created successfully: {backup_file}")
        return backup_file, True
    except subprocess.CalledProcessError as e:
        logging.error(f"Backup failed: {e.stderr.decode('utf-8', errors='replace')}")
        return str(e.stderr.decode('utf-8', errors='replace')), False
    except FileNotFoundError:
        logging.error("pg_dump no encontrado.")
        return "pg_dump no encontrado. ¿Está PostgreSQL instalado en la ruta por defecto?", False

def run_script(script_path):
    logging.info(f"Running script: {script_path}")
    
    try:
        # Run using the venv python
        python_exe = os.path.join(os.getcwd(), 'venv', 'Scripts', 'python.exe')
        result = subprocess.run([python_exe, script_path], check=True, capture_output=True, text=True)
        logging.info(f"Successfully ran {script_path}")
        return True, result.stdout
    except subprocess.CalledProcessError as e:
        logging.error(f"Error running {script_path}: {e.stderr}")
        return False, e.stderr

def main():
    start_time = datetime.now()
    logging.info("=== Starting Automated Run ===")
    
    report_body = f"""
    <h2>Reporte de Scraping Semanal - FarmaSearch</h2>
    <p><strong>Fecha de inicio:</strong> {start_time.strftime('%Y-%m-%d %H:%M:%S')}</p>
    """
    
    # 1. Backup DB
    backup_path, backup_success = backup_database()
    if backup_success:
        report_body += f"<p>✅ <strong>Backup DB:</strong> Éxito. Guardado en <code>{backup_path}</code></p>"
    else:
        report_body += f"<p>❌ <strong>Backup DB:</strong> Falló. Error: {backup_path}</p>"
        
    report_body += "<h3>Ejecución de Scrapers:</h3><ul>"
    
    # 2. Run Scrapers
    scrapers = [
        "scrapers/dosfarma.py",
        "scrapers/farmaciasdirect.py",
        "scrapers/promofarma.py",
        "scrapers/atida.py",
        "scrapers/vazquez.py"
    ]
    
    all_success = True
    for scraper in scrapers:
        if os.path.exists(scraper):
            success, output = run_script(scraper)
            if success:
                report_body += f"<li>✅ <strong>{scraper}</strong>: Ejecutado correctamente.</li>"
            else:
                report_body += f"<li>❌ <strong>{scraper}</strong>: Error en la ejecución. Ver logs.</li>"
                all_success = False
        else:
            report_body += f"<li>⚠️ <strong>{scraper}</strong>: Archivo no encontrado.</li>"
            
    report_body += "</ul>"
    
    # 3. Consolidar datos
    report_body += "<h3>Consolidación (Base de datos final):</h3>"
    success, output = run_script("scripts/consolidar_sql.py")
    if success:
        report_body += f"<p>✅ Consolidación en BD terminada correctamente.</p>"
        # Extraer números de la salida (esto depende de lo que printee consolidar_sql.py)
        # Por ahora lo ponemos genérico
        report_body += "<pre style='background:#f4f4f4;padding:10px;border-radius:5px;'>" + output.replace('<', '&lt;').replace('>', '&gt;') + "</pre>"
    else:
        report_body += f"<p>❌ Error consolidando datos.</p>"
        all_success = False
        
    end_time = datetime.now()
    duration = end_time - start_time
    
    report_body += f"""
    <hr>
    <p><strong>Fecha de fin:</strong> {end_time.strftime('%Y-%m-%d %H:%M:%S')}</p>
    <p><strong>Duración total:</strong> {duration}</p>
    <p><em>Este correo es autogenerado por el sistema de automatización Farmasearch.</em></p>
    """
    
    # 4. Send Email
    subject_prefix = "[✅ ÉXITO]" if all_success else "[⚠️ CON ERRORES]"
    subject = f"{subject_prefix} Reporte Scraping FarmaSearch - {start_time.strftime('%Y-%m-%d')}"
    
    send_email(subject, report_body)
    logging.info("=== Finished Automated Run ===")

if __name__ == "__main__":
    main()
