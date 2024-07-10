from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime as dt



with DAG (
    dag_id="File_Sensor",
    description="Sensor waiting for csv file",
    start_date=dt(2024, 5, 1),
    end_date = dt(2024, 5, 31),
    schedule_interval="@daily",
    max_active_runs=1
) as dag:
    

    execution_date = "{{ ds }}"

    # RAW PROCESS TRIGGER
    waiting_file_from_db = FileSensor(
        task_id="Waiting_db_file",
        filepath= f"/opt/airflow/temp_data/data_sales_{execution_date}.csv",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        depends_on_past=True
    )

    # CLEAN PROCESS TRIGGER
    waiting_raw_file = FileSensor(
        task_id="Waiting_raw_file",
        filepath= f"/opt/airflow/my_bucket/data/raw/data_sales_raw_{execution_date}.csv",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        depends_on_past=True
    )


    # CONSUMPTION PROCESS TRIGGER
    waiting_clean_file = FileSensor(
        task_id="Waiting_clean_file",
        filepath= f"/opt/airflow/my_bucket/data/clean/data_sales_clean_{execution_date}.csv",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        depends_on_past=True
    )


    # DELETE TEMP FILE PROCESS TRIGGER
    waiting_consumption_file = FileSensor(
        task_id="Waiting_consumption_file",
        filepath= f"/opt/airflow/my_bucket/data/consumption/data_sales_consumption_{execution_date}.csv",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        depends_on_past=True
    )




waiting_file_from_db >> waiting_raw_file >> waiting_clean_file >> waiting_consumption_file
