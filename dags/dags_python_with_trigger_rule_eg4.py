from airflow import DAG
import pendulum
from airflow.decorators import task
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_python_with_trigger_rule_eg2",
    start_date=pendulum.datetime(2025, 4, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False
) as dag:
    
    @task.branch(task_id='branhcing')
    def random_branch():
        import random
        item_lst = ['A', 'B', 'C']
        selected_item = random.choice(item_lst)
        if selected_item == 'A':
            return 'task_a'
        elif selected_item == 'B':
            return 'task_b'
        elif selected_item == 'C':
            return 'task_c'

    
    task_a = BashOperator(
        task_id='task_a',
        bash_command= 'echo upstream1'
    )
    
    @task
    def common_task():
        print('정상처리')
    
    task_b = common_task.override(task_id='task_b')
    task_c = common_task.override(task_id='task_c')
    task_d = common_task.override(task_id='task_d',trigger_rule='none_skipped')
    

    random_branch() >> [task_a, task_b, task_c] >> task_d