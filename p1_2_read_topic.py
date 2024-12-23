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

    # * Створення Spark сесії
    logger.info("Creating Spark session")

    spark = (
        SparkSession.builder.appName("EnhancedKafkaStreaming")
        .master("local[*]")
        .getOrCreate()
    )

    # * визначення схеми для даних
    schema = StructType(
        [
            StructField("sport", StringType(), True),
            StructField("medal", StringType(), True),
            StructField("sex", StringType(), True),
            StructField("noc_country", StringType(), True),
            StructField("avg_height", StringType(), True),
            StructField("avg_weight", StringType(), True),
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
        .option("subscribe", topic_names_dict["aggregated_results"])
        .option("startingOffsets", "earliest")
        # .option("maxOffsetsPerTrigger", "5")
        .option("maxOffsetsPerTrigger", "50")
        .option("failOnDataLoss", "false")
        .load()
        .withColumn("value", regexp_replace(col("value").cast("string"), "\\\\", ""))
        .withColumn("value", regexp_replace(col("value"), '^"|"$', ""))
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )

    # * Виведення отриманих даних на екран
    logger.info("Printing data to console")

    kafka_streaming_df.writeStream.trigger(availableNow=True).outputMode(
        "append"
    ).format("console").option("truncate", "false").start().awaitTermination()

    logger.info("Finish working")


if __name__ == "__main__":
    main()
