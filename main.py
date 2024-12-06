import requests
from bs4 import BeautifulSoup
from io import BytesIO
import zipfile
import os
import pandas as pd
import sqlite3

response = requests.get(
    "https://www.dgt.es/menusecundario/dgt-en-cifras/matraba-listados/conductores-autoescuelas.html")
soup = BeautifulSoup(response.text, 'html.parser')


def get_month_and_year(href: str):
    data = href.split("/")
    return data[5], data[6]


base_url = "https://www.dgt.es"
merged_data = []

# Directorio para almacenar los archivos extraídos
os.makedirs("downloads", exist_ok=True)

for i, link in enumerate(soup.find_all("a")):
    href = link.get("href")
    if href and href.endswith(".zip"):  # Asegurarse de que sea un archivo ZIP
        href = base_url + href if not href.startswith("http") else href
        try:
            year, month = get_month_and_year(href)
            response = requests.get(href, stream=True)
            response.raise_for_status()
            z = zipfile.ZipFile(BytesIO(response.content))
            extract_path = f"downloads/{year}_{month}"
            os.makedirs(extract_path, exist_ok=True)
            z.extractall(extract_path)  # Extraer archivos al directorio específico

            # Procesar cada archivo extraído
            for file_name in os.listdir(extract_path):
                if file_name.endswith(".txt"):  # Suponiendo que los archivos extraídos son .txt
                    file_path = os.path.join(extract_path, file_name)
                    data = pd.read_csv(file_path, sep=";", encoding="latin1")
                    # Filtrar registros donde DESC_provincia no sea "Palmas (Las)"
                    filtered_data = data[data["DESC_PROVINCIA"] == "Palmas (Las)"]
                    merged_data.append(filtered_data)

        except Exception as e:
            print(f"Error procesando {href}: {e}")

# Combinar todos los DataFrames filtrados
if merged_data:
    final_df = pd.concat(merged_data, ignore_index=True)

    # Guardar los datos en una base de datos SQLite
    conn = sqlite3.connect("filtered_data.db")  # Crear/abrir la base de datos
    final_df.to_sql("filtered_data", conn, if_exists="replace", index=False)  # Guardar la tabla
    conn.close()
    print("Datos filtrados guardados en 'filtered_data.db'.")
else:
    print("No hay datos para combinar.")
