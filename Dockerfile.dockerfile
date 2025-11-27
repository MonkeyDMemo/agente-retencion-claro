FROM python:3.11-slim

WORKDIR /app

# Copiar requirements primero (para cache de Docker)
COPY requirements.txt .

# Instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el resto del código
COPY streamlit.py .

# Puerto que usa Streamlit
EXPOSE 8080

# Comando de inicio
CMD ["streamlit", "run", "streamlit.py", "--server.port=8080", "--server.address=0.0.0.0", "--server.headless=true"]