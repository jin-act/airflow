from airflow.models.dag import DAG
import pendulum
from pprint import pprint
from airflow.operators.python import PythonOperator
from airflow.decorators import task

with DAG(
    dag_id="dags_python_self_test",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    # [START howto_operator_python]
    def regist(name, sex, *args, **kwargs):
        print(name)
        print(sex)
        print(args)
        print(kwargs)

    test_py_task_1 = PythonOperator(
        task_id='python_task_1',
        python_callable=regist, 
        op_args=['jsw','man','korea','seoul'],
        op_kwargs={
            'phone': '010',
            'email': 'jswsunwoo123@gmail.com'
            }
        )

    test_py_task_1