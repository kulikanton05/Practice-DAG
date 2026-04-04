from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime

from pendulum import datetime, duration
from pathlib import Path
import pandas as pd
import logging
import io


#Название репозитория = твой логин
REPOSITORY_NAME = Path(__file__).parent.name.replace(".", "_")
TASK = "etl1_t3"

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
    # CRON расписание
    schedule="*/15 * * * *",
    tags=['de_2026', TASK],
    default_args=DEFAULT_ARGS,
    catchup=False,
    dagrun_timeout=duration(minutes=60),
)
def pipeline():
    @task
    def s3_list_files(aws_src_conn_id=None):
        # Получаем контекст Task
        context = get_current_context()

        # Путь к файлу
        src_path = f"""files/public/data_engineering/data_streams/ds_avia_sim_ru/{context.get('ds')}/"""
        logging.info("PATH TO FILE: " + src_path)

        # Подключимся к S3
        s3 = S3Hook(aws_conn_id=aws_src_conn_id)

        # Получим весь список файлов в папке за сегодня
        files = s3.list_keys(
            bucket_name=S3_SRC_BUCKET_NAME,
            prefix=src_path
        )
        logging.info("FILES TO LOAD: " + src_path)

        return files


    @task
    def s3_daily_load(list_files, aws_src_conn_id, aws_dst_conn_id):
        # Получаем контекст Task
        context = get_current_context()

        # Скачаем файл
        s3_src = S3Hook(aws_conn_id=aws_src_conn_id)
        s3_dst = S3Hook(aws_conn_id=aws_dst_conn_id)

        if list_files is None:
            list_files = []

        df_list = []
        for file in list_files:
            local_file_name = s3_src.download_file(
                bucket_name = S3_SRC_BUCKET_NAME,
                key=file
            )
            df_list.append(pd.read_csv(local_file_name))

        # Сохраним результат в Dataframe
        df = pd.concat(df_list, ignore_index=True)
        logging.info(df.head())

        buffer = io.BytesIO()

        # Сохраняем данные в память в оптимизированном для аналитики формате parquet
        df.to_parquet(buffer, engine="pyarrow", index=False, compression="snappy")

        # Смещаем указатель буффера в начало
        buffer.seek(0)

        # Путь, куда пишем
        dst_path = f"""users/{REPOSITORY_NAME}/avia_daily/"""
        dst_file = f"""avia_{context.get('ds_nodash')}.snappy.parquet"""
        logging.info(dst_path + dst_file)

        # Сохраняем байты в S3
        s3_dst.load_bytes(
            bytes_data=buffer.getvalue(),
            bucket_name=S3_DST_BUCKET_NAME,
            key= dst_path + dst_file,
            replace=True
        )



    list_files = s3_list_files(
        aws_src_conn_id=f"public_files_s3"
    )

    s3_daily_load(
        list_files = list_files,
        aws_src_conn_id = "public_files_s3",
        aws_dst_conn_id = f"{REPOSITORY_NAME}_s3"
    )


pipeline()