from airflow.models.dag import DAG
import pendulum
from pprint import pprint
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="dags_python_task_decorator",
    schedule="0 2 * * 1",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    # [START howto_operator_python]
    def print_context(some_input):
        print(some_input)

    python_task_1 = PythonOperator(
        task_id='python_task_1',
        python_callable=print_context, op_args=['task_decorator 실행'])

    python_task_1
