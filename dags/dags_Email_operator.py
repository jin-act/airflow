from airflow import DAG
import datetime
import pendulum
from airflow.operators.email import EmailOperator

with DAG(
    dag_id="dags_Email_operator",
    schedule="0 8 1 * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    ) as dag:
    
    send_email_task = EmailOperator(
        task_id="send_email_task",
        to='jjin4073@naver.com',
        cc='jswsunwoo123@gmail.com',
        subject='Airflow 성공메일',
        html_content='Airflow 작업이 완료되었습니다.',
    )