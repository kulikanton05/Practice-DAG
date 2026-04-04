from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime

from pendulum import datetime, duration
from pathlib import Path
import pandas as pd
import logging


# Название репозитория = твой логин
REPOSITORY_NAME = Path(__file__).parent.name.replace(".", "_")

# Тема + задачи
TASK = "etl1_t1"

# Бакет S3
S3_BUCKET_NAME = "culab-files-prod"


#Аргументы для Task
DEFAULT_ARGS = {
    "owner": "de_course",
    "retries": 1,
    "retry_delay": duration(minutes=1),
    "execution_timeout": duration(seconds=300),
}

#Создаем DAG
@dag(
    dag_id=f"{REPOSITORY_NAME}_{TASK}",
    start_date=datetime(2025, 1, 1, tz="UTC"),
    # CRON расписание
    schedule="*/15 * * * *",
    tags=['de_2026', TASK],
    default_args=DEFAULT_ARGS,
    catchup=False,
    dagrun_timeout=duration(minutes=60),
)
def pipeline():
    @task
    def s3_last_update(src_aws_conn_id=None):
        # Получаем контекст Task
        context = get_current_context()

        # Путь к файлам
        src_path = f"""files/public/data_engineering/data_streams/ds_avia_sim_ru/{context.get("ds")}/"""
        logging.info("PATH TO FILE: " + src_path)

        # Подключимся к S3
        s3 = S3Hook(aws_conn_id=src_aws_conn_id)

        # Получим весь список файлов в папке за сегодня
        files = s3.list_keys(
            bucket_name= S3_BUCKET_NAME,
            prefix= "files/public/data_engineering/data_streams/ds_avia_sim_ru/"
        )
        last_file = max(files)
        logging.info("LAST FILE: " + last_file)

        return last_file


    @task
    def s3_read_file(last_file=None, src_aws_conn_id=None):
        # Скачаем файл
        s3 = S3Hook(aws_conn_id=src_aws_conn_id)

        local_file_name = s3.download_file(
            bucket_name=S3_BUCKET_NAME,
            key=last_file
        )
        logging.info("LOADED FILE: " + local_file_name)

        # Сохраним результат в Dataframe
        df = pd.read_csv(local_file_name)
        logging.info(f"LOADED: {len(df)} ROWS;\n FILE: {last_file}")
        logging.info(df.head())


    last_file = s3_last_update(
        src_aws_conn_id="public_files_s3"
    )

    s3_read_file(
        last_file=last_file,
        src_aws_conn_id="public_files_s3"
    )


pipeline() # что будет, если это не написать?