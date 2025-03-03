"""from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.utils.helpers import chain

def print_a():
    print('hi from task a')

def print_b():
    print('hi from task b')

def print_c():
    print('hi from task c')

def print_d():
    print('hi from task d')

def print_e():
    print('hi from task e')

default_args = {
    'retries': 3,
}

with DAG('my_dag', start_date=datetime(2025, 2 , 11), default_args=default_args,
         description='A simple tutorial DAG', tags=['data_science'],
         schedule='@daily', catchup=False):
    
    task_a = PythonOperator(task_id='task_a', python_callable=print_a)
    task_b = PythonOperator(task_id='task_b', python_callable=print_b)
    task_c = PythonOperator(task_id='task_c', python_callable=print_c)
    task_d = PythonOperator(task_id='task_d', python_callable=print_d)
    task_e = PythonOperator(task_id='task_e', python_callable=print_e)

    chain(task_a, [task_b, task_c], [task_d, task_e])"""


from airflow.decorators import dag, task
from datetime import datetime

default_args = {
    'retries': 3,
}

@dag(start_date=datetime(2025, 2 , 11), default_args=default_args,
         description='A simple tutorial DAG', tags=['data_science'],
         schedule='@daily', catchup=False)
def my_dag():

    @task
    def print_a():
        print('hi from task a')
    
    @task
    def print_b():
        print('hi from task b')

    @task
    def print_c():
        print('hi from task c')

    @task
    def print_d():
        print('hi from task d')

    @task
    def print_e():
        print('hi from task e')

    a = print_a()
    b = print_b()
    c = print_c()
    d = print_d()
    e = print_e()
    
    a >> [b, c]
    b >> d
    c >> e

my_dag()