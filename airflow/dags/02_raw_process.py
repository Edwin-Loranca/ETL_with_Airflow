from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator
from datetime import datetime as dt
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from etl_process.data_integration.integration import integrate_data


def integration(**kwargs):

    execution_date = kwargs['ds']  
    source_path = f"/opt/airflow/temp_data/data_sales_{execution_date}.csv"
    raw_path = f"/opt/airflow/my_bucket/data/raw/data_sales_raw_{execution_date}.csv"
    integrate_data(source_path, raw_path)


with DAG(
    dag_id="Integration_process",
    description="Functions to integrate data from source",
    start_date=dt(2024, 5, 1),
    end_date = dt(2024, 5, 31),
    schedule_interval="@daily",
    max_active_runs=1
) as dag:
    
    
    file_sensor = ExternalTaskSensor(
        task_id="file_from_source_ready",
        external_dag_id="File_Sensor",
        external_task_id="Waiting_db_file",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        poke_interval=10
    )

    
    file_ready_notification = BashOperator(
        task_id="file_from_source_ready_notification",
        bash_command="sleep 5 && echo 'The file from source db is ready for integration'"
    )


    integration_process = PythonOperator(
        task_id="execute_integrate_data_function",
        python_callable=integration,
        retries=2,
        retry_delay=5,
        depends_on_past=True
    )

    end_process = ExternalTaskSensor(
        task_id="Waiting_to_start_file_from_source_ready",
        external_dag_id="Delete_temp_data_process",
        external_task_id="delete_temp_data",
        poke_interval=10
    )



file_sensor >> file_ready_notification >> integration_process >> end_process