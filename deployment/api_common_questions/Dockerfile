FROM python:3.12-slim

# Definir el directorio de trabajo
WORKDIR /app

# Instalar dependencias necesarias del sistema
RUN apt-get update && apt-get upgrade -y && apt-get install -y procps && rm -rf /var/lib/apt/lists/*

COPY apis/ .

RUN pip install --no-cache-dir -r /app/requirements.txt

ENTRYPOINT ["python3", "/app/scheduler.py"]