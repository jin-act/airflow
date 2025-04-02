from airflow import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_operator",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    select_t1 = BashOperator(
        task_id="select_t1",
        bash_command="$ORANGE",
    )
       
    select_t2 = BashOperator(
        task_id="select_t2",
        bash_command="$AVOCADO",
    )
    
    select_t1 >> select_t2
    