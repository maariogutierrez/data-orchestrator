import time
import pandas as pd
from pymongo import MongoClient
from elasticsearch import Elasticsearch, helpers
import os
CSV_URL = "https://drive.google.com/uc?id=1NHGHPg1LzmCKhc7TYsg45aN5jfNhgr_4&export=download"
MONGO_URI = "mongodb://mongodb:27017/"
MONGO_DB = "taxi_data"
MONGO_COLLECTION = "data"
ES_URI = "http://elasticsearch:9200"
ES_INDEX = "data"

first_run = True
# Leemos el CSV
def load_data():
    global first_run
    print("\n=== Iniciando proceso de ingesta ===")
    df = pd.read_csv(CSV_URL)
    print(f"CSV cargado: {df.shape[0]} filas x {df.shape[1]} columnas")

    records = df.to_dict("records")

    # Cargamos datos en MongoDB
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client[MONGO_DB]
    mongo_col = mongo_db[MONGO_COLLECTION]


    
    # Limpiamos la colección antes de insertar (opcional)
    if first_run:
        mongo_col.delete_many({})
        print("Colección MongoDB vaciada (solo la primera vez).")
        
    existing_docs = mongo_col.count_documents({})
    new_records = records[existing_docs:]  
    
    if new_records:
        mongo_col.insert_many(new_records)
        print(f"\nSe han insertado {len(new_records)} nuevas filas en MongoDB.")
    else:
        print("\nNo hay nuevas filas para insertar en MongoDB (CSV sin cambios).")

    # Para Elasticsearch regeneramos records directamente desde el DataFrame en vez del sobrescribir sobre mongo
    records_for_es = df.to_dict("records")

    # Cargamos en Elasticsearch (sin usuario ni contraseña)
    es = Elasticsearch([ES_URI])

    # Crear índice si no existe
    if not es.indices.exists(index=ES_INDEX):
        es.indices.create(index=ES_INDEX)
        print(f"Índice '{ES_INDEX}' creado en Elasticsearch.")
        # Insertamos los documentos
        actions = [{"_index": ES_INDEX, "_source": record} for record in records_for_es]
        helpers.bulk(es, actions)
        print(f"CSV cargado en Elasticsearch ({len(records_for_es)} documentos).")

    first_run = False
    
    
if __name__ == "__main__":
    interval_minutes = int(os.getenv("INGEST_INTERVAL_MINUTES", "5"))
    print(f"Scheduler activo: ejecutando cada {interval_minutes} min.")
    while True:
        try:
            load_data()
        except Exception as e:
            print(f"[ERROR] Ingesta fallida: {e}")
        time.sleep(interval_minutes * 60)
        