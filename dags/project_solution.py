import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# Шлях до папки з дагами взяти з змінної середовища BASE_PATH з файлу docker-compose.yaml
DAGS_PATH = os.getenv("DAGS_PATH", "/opt/airflow/dags")


# Налаштування DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 12, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "PreArt_End-to-End_Batch_Data_Lake",
    default_args=default_args,
    description="PreArt. End-to-End Batch Data Lake",
    schedule_interval=timedelta(days=1),
)

# Завдання для запуску скриптів
landing_to_bronze = BashOperator(
    task_id="landing_to_bronze",
    bash_command=f"python {DAGS_PATH}/landing_to_bronze.py",
    dag=dag,
)

bronze_to_silver = BashOperator(
    task_id="bronze_to_silver",
    bash_command=f"python {DAGS_PATH}/bronze_to_silver.py",
    dag=dag,
)

silver_to_gold = BashOperator(
    task_id="silver_to_gold",
    bash_command=f"python {DAGS_PATH}/silver_to_gold.py",
    dag=dag,
)

# Визначення послідовності завдань
landing_to_bronze >> bronze_to_silver >> silver_to_gold
