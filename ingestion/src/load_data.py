import pandas as pd
from pymongo import MongoClient
from elasticsearch import Elasticsearch, helpers

CSV_URL = "https://drive.google.com/uc?id=1NHGHPg1LzmCKhc7TYsg45aN5jfNhgr_4&export=download"
MONGO_URI = "mongodb://mongodb:27017/"
MONGO_DB = "taxi_data"
MONGO_COLLECTION = "data"
ES_URI = "http://elasticsearch:9200"
ES_INDEX = "data"

# Leemos el CSV
df = pd.read_csv(CSV_URL)
print(f"\nCSV cargado desde Google Drive")
print(f"Forma: {df.shape[0]} filas x {df.shape[1]} columnas")
print("Primeras filas del CSV:")
print(df.head())

records = df.to_dict("records")

# Cargamos datos en MongoDB
mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client[MONGO_DB]
mongo_col = mongo_db[MONGO_COLLECTION]

# Limpiamos la colección antes de insertar (opcional)
mongo_col.delete_many({})
mongo_col.insert_many(records)
print(f"\nCSV cargado en MongoDB ({len(records)} documentos).")

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

