from airflow.models.dag import DAG
import pendulum
from pprint import pprint
from airflow.operators.python import PythonOperator
from airflow.decorators import task

with DAG(
    dag_id="dags_python_task_decorator",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2025, 3, 28, tz="Asia/Seoul"),
    catchup=true,
) as dag:
    
    @task(task_id="python_task")
    # [START howto_operator_python]
    def show_templates(**kwargs):
        pprint(kwargs)
  
    show_templates()