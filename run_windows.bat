@echo off
setlocal
cd /d %~dp0

if not exist venv (
  python -m venv venv
)

call venv\Scripts\activate

pip install -r requirements.txt

if "%1"=="test" (
    echo Ejecutando pruebas unitarias...
    python -m pytest -q
    exit /b
)

streamlit run app.py
