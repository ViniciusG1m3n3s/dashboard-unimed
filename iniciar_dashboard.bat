@echo off
echo Ativando ambiente virtual...
cd /d "%~dp0"
if exist venv\Scripts\activate (
    call venv\Scripts\activate
) else (
    echo Ambiente virtual não encontrado. Criando...
    python -m venv venv
    call venv\Scripts\activate
    pip install -r requirements.txt
)

echo Iniciando o dashboard...
streamlit run launcher.py
pause