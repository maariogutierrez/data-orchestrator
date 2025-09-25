import os
import pandas as pd
from pymongo import MongoClient
from elasticsearch import Elasticsearch, helpers

# Ruta del CSV descargado
CSV_PATH = "ingestion/data/Datos_format.csv"

# Verificamos que exista el CSV
if not os.path.exists(CSV_PATH):
    raise FileNotFoundError(
        "CSV no encontrado. Ejecute ingestion/src/open_csv.py primero."
    )

# Leemos el CSV
df = pd.read_csv(CSV_PATH)
records = df.to_dict("records")

# ----------------------------
# Conectar y cargar en MongoDB
# ----------------------------
mongo_client = MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["taxi_data"]
mongo_col = mongo_db["data"]

# Limpiar colección antes de insertar (opcional)
mongo_col.delete_many({})
mongo_col.insert_many(records)
print(f"CSV cargado en MongoDB ({len(records)} documentos).")

# ---------------------------------
# Conectar y cargar en Elasticsearch
# ---------------------------------
es = Elasticsearch(
    ["http://elasticsearch:9200"],
    basic_auth=("elastic", "prontotodoacabara")
)

# Crear índice si no existe
index_name = "data"
if not es.indices.exists(index=index_name):
    es.indices.create(index=index_name)
    print(f"Índice '{index_name}' creado en Elasticsearch.")

actions = [
    {"_index": index_name, "_source": record}
    for record in records
]
helpers.bulk(es, actions)
print(f"CSV cargado en Elasticsearch ({len(records)} documentos).")