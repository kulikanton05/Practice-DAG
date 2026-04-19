from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from pendulum import datetime, duration
from pathlib import Path
import pandas as pd
import logging
import io


REPOSITORY_NAME = Path(__file__).parent.name.replace(".", "_")
# Тема + задача
TASK = "etl2_t1"


# Аргументы для Task
DEFAULT_ARGS = {
    "owner": "de_course",
    "retries": 1,
    "retry_delay": duration(minutes=1),
    "execution_timeout": duration(seconds=300),
}

# Создаем DAG
@dag(
    dag_id=f"{REPOSITORY_NAME}_{TASK}",
    start_date=datetime(2026, 1, 1, tz="UTC"),
    schedule="0 0 * * *",
    tags=[TASK, "de_2026"],
    default_args=DEFAULT_ARGS,
    catchup=False,
    dagrun_timeout=duration(minutes=60),
)
def pipeline():
    @task
    def read_gp(gp_conn_id=None):
        # Получаем контекст Task
        context = get_current_context()

        # Подключаемся к Greenplum
        gp_hook = PostgresHook(postgres_conn_id=gp_conn_id)
        query = f"""
        select 
            (departure_time at TIME ZONE 'UTC')::date observation_dt,
            departure_airport,
            status,
            count(*) as cnt,
            (now() at TIME ZONE 'UTC')::timestamp as loaded_dttm
        from 
            databases_and_sql.avia_domestic_us
        where 
            departure_time at TIME ZONE 'UTC' >= '{context.get("data_interval_start")}' 
            and departure_time at TIME ZONE 'UTC' < '{context.get("data_interval_end")}' 
        group by 
            observation_dt, departure_airport, status
        order by 
            departure_airport, status;
        """



        # Делаем запрос, сохраняем Dataframe
        df_flights = gp_hook.get_df( ... )
        logging.info(df_flights.head(10))
        

    last_file = read_gp(
        gp_conn_id=f"{REPOSITORY_NAME}_gp"
    )


pipeline()
