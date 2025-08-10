Param()

$ErrorActionPreference = 'Stop'
Set-Location -Path $PSScriptRoot

Write-Host "[*] Verificando Python..."
try {
  $python = (Get-Command python -ErrorAction Stop).Source
} catch {
  Write-Error "Python no encontrado. Instale Python 3.10+ desde https://www.python.org/downloads/ y marque 'Add to PATH'."
  exit 1
}

if (-not (Test-Path .venv)) {
  Write-Host "[*] Creando entorno virtual .venv..."
  & $python -m venv .venv
}

Write-Host "[*] Activando entorno virtual..."
$env:VIRTUAL_ENV = Join-Path $PWD ".venv"
$env:PATH = (Join-Path $env:VIRTUAL_ENV "Scripts") + ";" + $env:PATH

Write-Host "[*] Actualizando pip e instalando dependencias..."
& .venv\Scripts\python.exe -m pip install --upgrade pip
& .venv\Scripts\pip.exe install -r requirements.txt

Write-Host "[*] Iniciando servidor Flask en una nueva ventana..."
Start-Process -FilePath ".venv\Scripts\python.exe" -ArgumentList "app.py" -WindowStyle Normal

Start-Process "http://127.0.0.1:5000"
Write-Host "[OK] Servidor iniciado."


