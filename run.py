import subprocess
import sys

print("Instalando dependencias...")
subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt", "--quiet"])

print("Iniciando aplicacion...")
subprocess.check_call([
    sys.executable, "-m", "streamlit", "run", "streamlit.py",
    "--server.port=8080",
    "--server.address=0.0.0.0", 
    "--server.headless=true"
])