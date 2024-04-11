from airflow import DAG
import pendulum
import random
import datetime
from airflow.operators.python import PythonOperator

with DAG(
    dag_id='dags_python_operator',
    start_date=pendulum.datetime(2023,3,1, tz='Asia/Seoul'), 
    schedule='30 6 * * *',
    catchup=False
) as dag:
    def select_fruit():
        frute = ['APPLE', 'BANANA', 'ORANGE', 'AVOCADO']
        rand_int = random.randint(0,3)
        print(frute[rand_int])

    py_t1 = PythonOperator( 
        task_id = 'py_t1',
        python_callable=select_fruit
    )

    py_t1