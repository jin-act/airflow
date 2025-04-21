from airflow import DAG
import pendulum
from airflow.operators.branch import BaseBranchOperator
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="dags_python_with_branch_decorator",
    start_date=pendulum.datetime(2025, 4, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    ) as dag:
    
    class CustomBranchOperator(BaseBranchOperator):
        #반드시 choose_branch를 사용하고, 파라미터 context를 넣어야 한다.
        def choose_branch(self, context):
            import random
            
            print(context)
            #print(context)는 실습 내용과는 관련 없지만, 사용하지 않는 context가 어떤 값인지 확인하기 위해 코딩
            
            item_lst = ['A', 'B', 'C']
            selected_item = random.choice(item_lst)
            if selected_item == 'A':
                return 'task_a'
            elif selected_item in ['B', 'C']:
                return ['task_b', 'task_c']

    custom_brnach_operator = CustomBranchOperator(task_id='python_branch_task')
    
    def common_func(**kwargs):
        print(kwargs['selected'])
    
    task_a = PythonOperator(
        task_id='task_a',
        python_callable=common_func,
        op_kwargs={'selected':'A'}
    )
    
    task_b = PythonOperator(
        task_id='task_b',
        python_callable=common_func,
        op_kwargs={'selected':'B'}
    )
    
    task_c = PythonOperator(
        task_id='task_c',
        python_callable=common_func,
        op_kwargs={'selected':'C'}
    )

    custom_brnach_operator >> [task_a, task_b, task_c]