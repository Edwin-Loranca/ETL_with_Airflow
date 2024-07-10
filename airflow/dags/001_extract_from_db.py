from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime as dt
from airflow.sensors.external_task import ExternalTaskSensor


with DAG (
    dag_id="Load_temp_file",
    description="Load db_source file to temp data",
    start_date=dt(2024, 5, 1),
    end_date = dt(2024, 5, 31),
    schedule_interval="@daily",
    max_active_runs=1
) as dag:
    
    execution_date = "{{ ds }}"

    load_temp_file = BashOperator(
        task_id="load_temp_data",
        bash_command=f"echo 'Copying file...' && sleep 5 && cp /opt/airflow/data_source/data_sales.csv /opt/airflow/temp_data/data_sales_{execution_date}.csv"
    )

    load_temp_file_notification = BashOperator(
        task_id="load_temp_data_notification",
        bash_command="sleep 5 && echo 'The file has been loaded with success...!!'"
    )

    end_process = ExternalTaskSensor(
        task_id="Waiting_to_start_load_temp_data",
        external_dag_id="Delete_temp_data_process",
        external_task_id="delete_temp_data",
        poke_interval=10
    )


load_temp_file >> load_temp_file_notification >> end_process