import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, regexp_replace


# Функція для чистки тексту
def clean_text(df):
    text_columns = [
        field.name for field in df.schema.fields if field.dataType == "StringType"
    ]
    for column in text_columns:
        df = df.withColumn(
            # column, trim(lower(regexp_replace(col(column), "[^a-zA-Z0-9\s]", "")))
            column,
            trim(lower(col(column))),
        )
    return df


# Ініціалізація SparkSession
spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

# Шлях до папки bronze
bronze_path = "/tmp/data/bronze"
silver_path = "/tmp/data/silver"

tables = ["athlete_bio", "athlete_event_results"]
for table in tables:
    # Зчитування таблиць з папки bronze
    df = spark.read.parquet(os.path.join(bronze_path, table))

    # Очистка
    df = clean_text(df)

    # Дедублікація
    df = df.dropDuplicates()

    # Збереження у форматі Parquet у папку silver/{table}
    df.write.parquet(os.path.join(silver_path, table), mode="overwrite")
    print(f"Writing {table} to {silver_path} finished")
    df_parquet = spark.read.parquet(os.path.join(silver_path, table))
    df_parquet.show(truncate=False)


# Завершення роботи SparkSession
spark.stop()
