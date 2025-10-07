FROM python:3.10-slim

# Instalar dependencias del sistema necesarias para compilar librerías Python
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Crear directorio de trabajo
WORKDIR /app

# Copiar primero requirements.txt (para aprovechar la cache de Docker)
COPY requirements.txt .

# Instalar dependencias del proyecto
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copiar el resto del proyecto Rasa
COPY . /app

# Puerto por defecto de Rasa
EXPOSE 5005

# Comando por defecto
RUN apt-get update && apt-get install -y netcat-openbsd
RUN chmod +x /app/start.sh
CMD ["bash", "/app/start.sh"]