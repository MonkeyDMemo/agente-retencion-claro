#!/bin/bash
echo "Instalando dependencias..."
python3 -m pip install --user -r requirements.txt

echo "Iniciando Streamlit..."
python3 -m streamlit run streamlit.py --server.port=8080 --server.address=0.0.0.0 --server.headless=true