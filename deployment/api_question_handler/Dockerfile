FROM python:3.12-slim

COPY questions /.
COPY common_questions /.
# Definir el directorio de trabajo
WORKDIR /app

# Instalar dependencias necesarias del sistema
RUN apt-get update && apt-get upgrade -y && apt-get install -y procps && rm -rf /var/lib/apt/lists/*

COPY cualapi/ .
RUN pip install --no-cache-dir -r app/requirements.txt

ENTRYPOINT ["python3", "/app/question_handler_api.py"]