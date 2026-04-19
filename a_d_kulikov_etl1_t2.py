from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime

from pendulum import datetime, duration
from pathlib import Path
import pandas as pd
import logging
import io


REPOSITORY_NAME = Path(__file__).parent.name.replace(".", "_")
TASK = "etl1_t2"
S3_SRC_BUCKET_NAME = "culab-files-prod"
S3_DST_BUCKET_NAME = "culab-student-files"


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

        # Путь к файлу
        src_path = f"""files/public/data_engineering/data_streams/ds_avia_sim_ru/{context.get("ds")}"""
        logging.info("PATH TO FILE: " + src_path)

        # Подключимся к S3
        s3 = S3Hook(aws_conn_id=src_aws_conn_id)

        # Получим весь список файлов в папке за сегодня
        files = s3.list_keys(
            bucket_name= S3_SRC_BUCKET_NAME,
            prefix= src_path
        )

        last_file = max(files)
        logging.info("LAST FILE: " + last_file)

        return last_file


    @task
    def s3_copy_file(last_file=None, src_aws_conn_id=None, dst_aws_conn_id=None):
        # Получаем контекст Task
        context = get_current_context()

        # Скачаем файл
        s3_src = S3Hook(aws_conn_id=src_aws_conn_id)

        local_file_name = s3_src.download_file(
            bucket_name=S3_SRC_BUCKET_NAME,
            key=last_file
        )
        logging.info("LOADED FILE: " + local_file_name)

        # Сохраним результат в Dataframe
        df = pd.read_csv(local_file_name)
        logging.info(df.head())

        buffer = io.BytesIO()

        # Сохраняем CSV в памяти
        df.to_csv(buffer, index=False)

        # Смещаем указатель буфера в начало
        buffer.seek(0)

        # Путь, куда пишем
        dst_path = f"""users/{REPOSITORY_NAME}/avia/{context.get("ds")}/"""
        dst_file = Path(last_file).name
        logging.info(dst_path + dst_file)

        s3_dst = S3Hook(aws_conn_id=src_aws_conn_id)

        # Сохраняем байты в S3
        s3_dst.load_bytes(
            bytes_data=buffer.getvalue(),
            bucket_name=S3_DST_BUCKET_NAME,
            key=dst_path + dst_file,
            replace=True
        )

    last_file = s3_last_update(
        src_aws_conn_id="public_files_s3"
    )

    s3_copy_file(
        last_file=last_file,
        src_aws_conn_id="public_files_s3",
        dst_aws_conn_id=f"{REPOSITORY_NAME}_s3"
    )

pipeline()
