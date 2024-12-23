import logging
import os  # Імпортуємо модуль os для роботи з операційною системою

from colorama import Fore, Style, init
from pyspark.sql import SparkSession  # Імпортуємо SparkSession з pyspark.sql
from pyspark.sql.functions import *  # Імпортуємо всі функції з pyspark.sql.functions
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (  # Імпортуємо типи даних з pyspark.sql.types
    DoubleType,
    IntegerType,
    StructField,
    StructType,
)

from configs import kafka_config  # Імпортуємо kafka_config з файлу configs
from configs import my_name, topic_names_dict

# Initialize colorama
init(autoreset=True)


# Configure logging with a custom formatter
class ColoredFormatter(logging.Formatter):
    COLORS = {
        "WARNING": Fore.YELLOW,
        "ERROR": Fore.RED,
        "INFO": Fore.GREEN,
        "DEBUG": Fore.BLUE,
    }

    def format(self, record):
        if record.levelname in self.COLORS:
            record.msg = f"{self.COLORS[record.levelname]}{record.msg}{Style.RESET_ALL}"
        return super().format(record)


# Set up logging
logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
handler.setFormatter(ColoredFormatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)
logger.setLevel(logging.INFO)


def main():
    """Main execution function."""
    logger.info("Start working")

    # Set up Spark packages
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 pyspark-shell"
    )
    # * Налаштування конфігурації SQL бази даних
    jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
    jdbc_table = "athlete_event_results"
    jdbc_user = "neo_data_admin"
    jdbc_password = "Proyahaxuqithab9oplp"

    # * Створення Spark сесії
    logger.info("Creating Spark session")
    
    # spark = (
    #     SparkSession.builder.config("spark.jars", "mysql-connector-j-8.0.32.jar")
    #     .appName("EnhancedKafkaStreaming")
    #     .getOrCreate()
    # )

    spark = (
        SparkSession.builder.config(
            "spark.jars", "mysql-connector-j-8.0.32.jar"
        )  # Додаємо MySQL конектор
        .config(
            "spark.sql.streaming.checkpointLocation", "checkpoint"
        )  # Вказуємо шлях для збереження контрольних точок
        .config(
            "spark.sql.streaming.forceDeleteTempCheckpointLocation", "true"
        )  # Дозволяємо видалення тимчасових контрольних точок
        .appName("EnhancedKafkaStreaming")  # Встановлюємо ім'я для застосунку
        .master("local[*]")  # Використовуємо всі доступні ядра на локальній машині
        .getOrCreate()  # Створюємо або отримуємо наявний SparkSession
    )

    # * Версії Spark
    logger.info("Spark version %s initialized", spark.version)

    # * Читання даних з SQL бази даних athlete_event_results
    logger.info("Reading data from MySQL database table athlete_event_results")
    jdbc_df = (
        spark.read.format("jdbc")
        .options(
            url=jdbc_url,  # URL бази даних
            driver="com.mysql.cj.jdbc.Driver",  # Драйвер для підключення до MySQL
            dbtable=jdbc_table,  # Таблиця, з якої читаємо дані
            user=jdbc_user,  # Ім'я користувача для підключення
            password=jdbc_password,  # Пароль для підключення
            partitionColumn="result_id",  # Колонка для розподілу даних на партиції
            lowerBound=1,  # Нижня межа значень для партицій
            upperBound=1000000,  # Верхня межа значень для партицій
            numPartitions="10",  # Кількість партицій = 10
        )
        .load()
    )

    # * Відправка даних до Kafka
    logger.info("Sending data to Kafka")

    jdbc_df.selectExpr(
        "CAST(result_id AS STRING) AS key", "to_json(struct(*)) AS value"
    ).write.format("kafka").option(
        "kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0]
    ).option(
        "kafka.security.protocol", kafka_config["security_protocol"]
    ).option(
        "kafka.sasl.mechanism", kafka_config["sasl_mechanism"]
    ).option(
        "kafka.sasl.jaas.config", kafka_config["sasl_jaas_config"]
    ).option(
        "topic", topic_names_dict["athlete_event_results"]
    ).save()

    # * Визначення схеми для JSON-даних
    schema = StructType(
        [
            StructField("athlete_id", IntegerType(), True),
            StructField("sport", StringType(), True),
            StructField("medal", StringType(), True),
            StructField("timestamp", StringType(), True),
        ]
    )

    # * Читання даних з Kafka
    logger.info("Reading data from Kafka")

    kafka_streaming_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0])
        .option("kafka.security.protocol", kafka_config["security_protocol"])
        .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
        .option("kafka.sasl.jaas.config", kafka_config["sasl_jaas_config"])
        .option("subscribe", topic_names_dict["athlete_event_results"])
        .option("startingOffsets", "earliest")
        # .option("maxOffsetsPerTrigger", "5")
        .option("maxOffsetsPerTrigger", "50000")
        .option("failOnDataLoss", "false")
        .load()
        .withColumn("value", regexp_replace(col("value").cast("string"), "\\\\", ""))
        .withColumn("value", regexp_replace(col("value"), '^"|"$', ""))
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.athlete_id", "data.sport", "data.medal")
    )

    # # * Виведення отриманих даних на екран
    # kafka_streaming_df.writeStream.trigger(availableNow=True).outputMode(
    #     "append"
    # ).format("console").option("truncate", "false").start().awaitTermination()

    # * Читання даних з SQL бази даних athlete_bio
    logger.info("Reading data from MySQL database table athlete_bio")

    athlete_bio_df = (
        spark.read.format("jdbc")
        .options(
            url=jdbc_url,  # URL бази даних
            driver="com.mysql.cj.jdbc.Driver",  # Драйвер для підключення до MySQL
            dbtable="athlete_bio",  # Таблиця, з якої читаємо дані
            user=jdbc_user,  # Ім'я користувача для підключення
            password=jdbc_password,  # Пароль для підключення
            partitionColumn="athlete_id",  # Колонка для розподілу даних на партиції
            lowerBound=1,  # Нижня межа значень для партицій
            upperBound=1000000,  # Верхня межа значень для партицій
            numPartitions="10",  # Кількість партицій = 10
        )
        .load()
    )

    # todo 2. Відфільтрувати дані, де показники зросту та ваги є порожніми або не є числами.
    logger.info("Filtering data")

    athlete_bio_df = athlete_bio_df.filter(
        # athlete_bio_df["height"].cast(DoubleType()).isNotNull()
        # & athlete_bio_df["weight"].cast(DoubleType()).isNotNull()
        (col("height").isNotNull())
        & (col("weight").isNotNull())
        & (col("height").cast("double").isNotNull())
        & (col("weight").cast("double").isNotNull())
    )

    # todo 4. Об’єднати дані з результатами змагань з Kafka-топіку з біологічними даними з MySQL таблиці за допомогою ключа athlete_id.
    logger.info("Joining data")

    joined_df = kafka_streaming_df.join(athlete_bio_df, "athlete_id")

    # todo 5. Знайти середній зріст і вагу атлетів індивідуально для кожного виду спорту, типу медалі або її відсутності, статі, країни (country_noc). Додайте також timestamp, коли розрахунки були зроблені.
    logger.info("Calculating average height and weight")

    aggregated_df = joined_df.groupBy("sport", "medal", "sex", "country_noc").agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight"),
        current_timestamp().alias("timestamp"),
    )

    # todo 6. Зробіть стрим даних (за допомогою функції forEachBatch) у: а) вихідний кафка-топік, b) базу даних.
    def write_to_kafka_and_mysql(df, epoch_id):
        # Write to Kafka
        df.selectExpr(
            "CAST(NULL AS STRING) AS key", "to_json(struct(*)) AS value"
        ).write.format("kafka").option(
            "kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0]
        ).option(
            "kafka.security.protocol", kafka_config["security_protocol"]
        ).option(
            "kafka.sasl.mechanism", kafka_config["sasl_mechanism"]
        ).option(
            "kafka.sasl.jaas.config", kafka_config["sasl_jaas_config"]
        ).option(
            "topic", topic_names_dict["aggregated_results"]
        ).save()

        # Write to MySQL
        df.write.format("jdbc").options(
            url=jdbc_url,
            driver="com.mysql.cj.jdbc.Driver",
            dbtable=topic_names_dict["aggregated_results"],
            user=jdbc_user,
            password=jdbc_password,
        ).mode("append").save()

    # * Запуск стріму
    logger.info("Starting streaming")

    query = (
        aggregated_df.writeStream.outputMode("complete")
        .foreachBatch(write_to_kafka_and_mysql)
        .option("checkpointLocation", "/path/to/checkpoint/dir")
        .start()
    )
    query.awaitTermination()

    # * Зупинка Spark сесії
    logger.info("Stopping Spark session")
    # spark.stop()

    logger.info("Finish working")


if __name__ == "__main__":
    main()
