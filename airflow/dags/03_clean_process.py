from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator
from datetime import datetime as dt
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from etl_process.clean_process.extract import read_data_from_raw_stage
from etl_process.clean_process.transformations_to_clean import *
from etl_process.clean_process.load_to_clean_stage import to_clean_stage


def processing_raw_file(**kwargs):
    execution_date = kwargs['ds']
    raw_path = f"/opt/airflow/my_bucket/data/raw/data_sales_raw_{execution_date}.csv"
    clean_path = f"/opt/airflow/my_bucket/data/clean/data_sales_clean_{execution_date}.csv"

    df = read_data_from_raw_stage(raw_path)
    df = clean_extra_spaces(df)
    df = replace_null_values(df)
    df =del_duplicates(df)
    df = create_load_date_column(df)
    to_clean_stage(df, clean_path)



with DAG(
    dag_id="Raw_data_process",
    description="Functions to processing raw data",
    start_date=dt(2024, 5, 1),
    end_date = dt(2024, 5, 31),
    schedule_interval="@daily",
    max_active_runs=1
) as dag:
    

    file_sensor = ExternalTaskSensor(
    task_id="raw_file_ready",
    external_dag_id="File_Sensor",
    external_task_id="Waiting_raw_file",
    trigger_rule=TriggerRule.ALL_SUCCESS,
    poke_interval=10
    )


    raw_file_ready_notification = BashOperator(
        task_id="raw_file_ready_notification",
        bash_command="sleep 5 && echo 'The file in raw stage is ready..!'"
    )


    processing_raw_file = PythonOperator(
        task_id="execute_raw_date_processing_tasks",
        python_callable=processing_raw_file,
        retries=2,
        retry_delay=5,
        depends_on_past=True
    )

    end_process = ExternalTaskSensor(
        task_id="Waiting_to_start_raw_file_ready",
        external_dag_id="Delete_temp_data_process",
        external_task_id="delete_temp_data",
        poke_interval=10
    )

file_sensor >> raw_file_ready_notification >> processing_raw_file >> end_process
