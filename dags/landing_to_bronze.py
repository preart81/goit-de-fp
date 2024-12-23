import os

import requests
from pyspark.sql import SparkSession


# * Функція для завантаження файлу з FTP-сервера
def download_file(url, local_path):
    """
    Download file from FTP server
    :param url: URL to file
    :param local_path: Local path to save file
    """
    response = requests.get(url, timeout=10)
    if response.status_code != 200:
        exit(f"Failed to download {url}. Status code: {response.status_code}")
    else:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "wb") as file:
            file.write(response.content)
            print(f"Downloaded {url} to {local_path}")


# * Ініціалізація SparkSession
spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()

# * Завантаження файлів з FTP-сервера
URL = "https://ftp.goit.study/neoversity/"
# ! Завантаження файлів з git-hub якщо FTP-сервер недоступний
URL = "https://github.com/yevheniihorbatyuk/DE_BLENDED/raw/refs/heads/main/"


tables = ["athlete_bio", "athlete_event_results"]

for table in tables:
    table_src = URL + table + ".csv"
    table_local_path = f"/tmp/{table}.csv"

    download_file(table_src, table_local_path)

    # Читання CSV-файлів за допомогою Spark
    df = spark.read.csv(table_local_path, header=True, inferSchema=True)

    # Збереження у форматі Parquet у папку bronze/{table}
    output_path = f"/tmp/data/bronze/{table}"
    os.makedirs(output_path, exist_ok=True)
    df.write.parquet(output_path, mode="overwrite")
    print(f"Writing {table} to {output_path} finished")

    df_parquet = spark.read.parquet(output_path)
    df_parquet.show(truncate=False)


# Завершення роботи SparkSession
spark.stop()
