import gdown
import pandas as pd
import os

def download_file_from_google_drive(file_id, destination):

    os.makedirs(os.path.dirname(destination), exist_ok=True)
    url = f'https://drive.google.com/uc?id={file_id}'
    gdown.download(url, destination, quiet=False)

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

    file_id = '1NHGHPg1LzmCKhc7TYsg45aN5jfNhgr_4'
    csv_file_path = 'ingestion/data/Datos_format.csv'
    
    download_file_from_google_drive(file_id, csv_file_path)
    
    load_and_preview_csv(csv_file_path)