FROM python:3.12-slim

# Definir el directorio de trabajo
WORKDIR /app

# Instalar dependencias necesarias del sistema
RUN apt-get update && apt-get upgrade -y && apt-get install -y procps && rm -rf /var/lib/apt/lists/*

COPY apis/ ./apis
RUN pip install -r requirements.txt

ENTRYPOINT ["python3", "/app/apis/scheduler.py"]