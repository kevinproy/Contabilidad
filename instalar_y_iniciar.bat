@echo off
setlocal ENABLEDELAYEDEXPANSION
cd /d "%~dp0"

echo [*] Verificando Python...
where python >nul 2>nul
if %errorlevel% neq 0 (
  echo [ERROR] Python no encontrado. Instale Python 3.10+ desde https://www.python.org/downloads/ y marque "Add to PATH".
  pause
  exit /b 1
)

if not exist .venv (
  echo [*] Creando entorno virtual .venv...
  python -m venv .venv
)

echo [*] Activando entorno virtual...
call .venv\Scripts\activate

echo [*] Actualizando pip e instalando dependencias...
python -m pip install --upgrade pip
pip install -r requirements.txt
if %errorlevel% neq 0 (
  echo [ERROR] Fallo instalando dependencias.
  pause
  exit /b 1
)

echo [*] Iniciando servidor Flask en segundo plano...
start "Servidor Flask" cmd /c ".venv\Scripts\python.exe app.py"

timeout /t 2 >nul
echo [*] Abriendo navegador en http://127.0.0.1:5000
start "" http://127.0.0.1:5000

echo [OK] Servidor iniciado. Esta ventana ya se puede cerrar.
endlocal


