import pendulum
from datetime import timedelta

from airflow.datasets import Dataset
from airflow.models import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}


with DAG(dag_id='clederson_dag',
         start_date=pendulum.datetime(2024,9,15),
         schedule='@daily',
         tags=['test'],
         default_args=default_args
         ) as dag:
    op1 = BashOperator(bash_command="touch ~/file_dag_generated.txt", 
                 task_id="create_file")
    op2 = BashOperator(bash_command="mv ~/file_dag_generated.txt{,.old}", 
                 task_id="rename_file")
    
    op1 >> op2


if __name__ == '__name__':
    dag.start()
    