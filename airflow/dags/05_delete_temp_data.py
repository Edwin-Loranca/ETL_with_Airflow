from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime as dt


with DAG(
    dag_id="Delete_temp_data_process",
    description="Delete temp file from temp_data",
    start_date=dt(2024, 5, 1),
    end_date = dt(2024, 5, 31),
    schedule_interval="@daily",
    max_active_runs=1
) as dag:
    
    execution_date = "{{ ds }}"    

    file_sensor = ExternalTaskSensor(
    task_id="clean_file_ready",
    external_dag_id="File_Sensor",
    external_task_id="Waiting_consumption_file",
    trigger_rule=TriggerRule.ALL_SUCCESS,
    poke_interval=10
    )

    end_process_notification = BashOperator(
        task_id="end_process_notification",
        bash_command="sleep 5 && echo 'The process has been successful...!'"
    )


    delete_temp_data = BashOperator(
        task_id="delete_temp_data",
        bash_command=f"sleep 5 && rm /opt/airflow/temp_data/data_sales_{execution_date}.csv"
    )

file_sensor >> end_process_notification >> delete_temp_data