#!/bin/bash
set -e

# 1. Lanzar las actions en segundo plano
rasa run actions &
echo "Iniciando Rasa Actions..."
sleep 5

# Esperar hasta que el puerto de actions (por defecto 5055) esté abierto
while ! nc -z localhost 5055; do
  echo "Esperando a que actions se inicie..."
  sleep 2
done
echo "Actions iniciadas ✅"

# 2. Lanzar el servidor principal en segundo plano
rasa run &
echo "Iniciando Rasa Server..."
sleep 5

# Esperar hasta que el puerto 5005 esté abierto
while ! nc -z localhost 5005; do
  echo "Esperando a que Rasa Server se inicie..."
  sleep 2
done
echo "Servidor Rasa iniciado ✅"

# 3. Ejecutar la app
python app.py
