@echo off
echo ===================================================
echo Iniciando Automatización de Scraping (FarmaSearch)
echo ===================================================

cd /d "c:\Users\danis\Desktop\Farmacia"

echo Activando entorno virtual...
call venv\Scripts\activate.bat

echo Ejecutando script principal (run_all.py)...
python run_all.py

echo.
echo ===================================================
echo Proceso de scraping finalizado. Revisa tu correo.
echo ===================================================
pause
