import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp

# Ініціалізація SparkSession
spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

# Шлях до папки silver
silver_path = "/tmp/data/silver"

# Зчитування таблиць з папки silver
athlete_bio_df = spark.read.parquet(os.path.join(silver_path, "athlete_bio"))
athlete_event_results_df = spark.read.parquet(
    os.path.join(silver_path, "athlete_event_results")
)
# видалити колонку athlete_event_results_df.country_noc
athlete_event_results_df = athlete_event_results_df.drop("country_noc")


# Виконання join за колонкою athlete_id
joined_df = athlete_bio_df.join(athlete_event_results_df, on="athlete_id")

# Обчислення середніх значень weight і height для кожної комбінації sport, medal, sex, country_noc
avg_stats_df = joined_df.groupBy("sport", "medal", "sex", "country_noc").agg(
    avg("weight").alias("avg_weight"), avg("height").alias("avg_height")
)

# Додавання колонки timestamp з часовою міткою виконання програми
avg_stats_df = avg_stats_df.withColumn("timestamp", current_timestamp())

# Збереження у форматі Parquet у папку gold/avg_stats
gold_path = "/tmp/data/gold"

avg_stats_df.write.parquet(os.path.join(gold_path, "avg_stats"), mode="overwrite")
print(f"Writing avg_stats to {gold_path} finished")
df_parquet = spark.read.parquet(os.path.join(gold_path, "avg_stats"))
df_parquet.show(truncate=False)

# Завершення роботи SparkSession
spark.stop()
