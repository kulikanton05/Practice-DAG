from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from pendulum import datetime, duration
from pathlib import Path
import pandas as pd
import logging


# Название репозитория = Ваш логин
REPOSITORY_NAME = Path(__file__).parent.name.replace(".", "_")
# Тема + задача
TASK = "etl2_t2"


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
    def attribute_data(gp_conn_id=None, pg_conn_id=None):
        # Получаем контекст Task
        context = get_current_context()

        # Подключение к Greenplum
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
        df_flights = gp_hook.get_df(query)


        # Подключение к Postgres
        pg_hook = PostgresHook(postgres_conn_id=pg_conn_id)
        query = """
        select type, name, iso_region, iata_code
        from data_engineering.airports_full
        """
        df_airports = pg_hook.get_df(query)

        # Объединяем данные
        df_flights = df_flights.merge(
            df_airports,
            left_on="departure_airport",
            right_on="iata_code",
            how="inner",
            suffixes=("_flights", "_airports")
        )[["observation_dt", "departure_airport", "type", "name", "iso_region", "status", "cnt", "loaded_dttm"]]

        

    last_file = attribute_data(
        gp_conn_id=f"{REPOSITORY_NAME}_gp",
        pg_conn_id=f"{REPOSITORY_NAME}_pg"
    )


pipeline()