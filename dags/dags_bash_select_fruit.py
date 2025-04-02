from airflow import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_select_fruit",
    schedule="10 0 * * 6#1",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    select_t1 = BashOperator(
        task_id="select_t1",
        bash_command="/opt/airflow/plugins/shell/select_fruit.sh ORANGE",    )
       
    select_t2 = BashOperator(
        task_id="select_t2",
        bash_command="/opt/airflow/plugins/shell/select_fruit.sh AVOCADO",
    )
    
    select_t1 >> select_t2
    