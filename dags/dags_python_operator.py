from airflow import DAG
import datetime
import pendulum
from airflow.operators.python import PythonOperator
#랜덤 함수에 반드시 필요한 선언문
import random

with DAG(
    dag_id="dags_python_operator",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    #python operatorn는 함수를 실행시키는 오퍼레이터
    #따라서 다음 내용은 python에서 함수를 정의하기 위한 작업 
    #함수를 정의 하기 위해선 def를 사용한다.
    #파이썬은 들여쓰기 문법을 사용함으로, 반드시 구간과 들여쓰기를 신경써서 작업해야 한다.
    def select_fruit():
        fruit = ['APPLE', 'BANANA', 'ORANGE', 'AVOCADO']
        rand_int = random.randint(0,3)
        print(fruit[rand_int])

    py_t1 = PythonOperator(
        task_id='py_t1',
        python_callable=select_fruit
    )

    py_t1