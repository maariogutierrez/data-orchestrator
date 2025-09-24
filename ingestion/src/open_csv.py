# ingestion/src/open_csv.py
import pandas as pd
import os

def load_and_preview_csv(csv_path):
    if not os.path.exists(csv_path):
        print(f"El archivo {csv_path} no existe.")
        return None

    df = pd.read_csv(csv_path)
    print(f"CSV cargado: {csv_path}")
    print(f"Forma: {df.shape[0]} filas x {df.shape[1]} columnas")
    print("Primeras filas del CSV:")
    print(df.head())
    return df

if __name__ == "__main__":
    csv_file_path = "ingestion/data/rows.csv"
    load_and_preview_csv(csv_file_path)
