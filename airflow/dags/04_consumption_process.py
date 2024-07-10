from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator
from datetime import datetime as dt
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from etl_process.consumption_process.extract import read_from_clean_stage
from etl_process.consumption_process.transformations_to_consumption import sales_by_publisher
from etl_process.consumption_process.load_to_consumption import load_to_consumption_stage




def processing_clean_file(**kwargs):
    execution_date = kwargs['ds']  
    clean_path = f"/opt/airflow/my_bucket/data/clean/data_sales_clean_{execution_date}.csv"
    consumption_path = f"/opt/airflow/my_bucket/data/consumption/data_sales_consumption_{execution_date}.csv"

    df = read_from_clean_stage(clean_path)
    df = sales_by_publisher(df)
    load_to_consumption_stage(df, consumption_path)



with DAG(
    dag_id="Clean_data_process",
    description="Functions to processing clean data",
    start_date=dt(2024, 5, 1),
    end_date = dt(2024, 5, 31),
    schedule_interval="@daily",
    max_active_runs=1
) as dag:
    

    file_sensor = ExternalTaskSensor(
    task_id="clean_file_ready",
    external_dag_id="File_Sensor",
    external_task_id="Waiting_clean_file",
    trigger_rule=TriggerRule.ALL_SUCCESS,
    poke_interval=10
    )


    clean_file_ready_notification = BashOperator(
        task_id="clean_file_ready_notification",
        bash_command="sleep 5 && echo 'The file in clean stage is ready..!'"
    )


    processing_clean_file = PythonOperator(
        task_id="execute_clean_data_processing_tasks",
        python_callable=processing_clean_file,
        retries=2,
        retry_delay=5,
        depends_on_past=True
    )

    end_process = ExternalTaskSensor(
        task_id="Waiting_to_start_clean_file_ready",
        external_dag_id="Delete_temp_data_process",
        external_task_id="delete_temp_data",
        poke_interval=10
    )


file_sensor >> clean_file_ready_notification >> processing_clean_file >> end_process