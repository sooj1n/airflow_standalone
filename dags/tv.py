from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
with DAG(
    'tv',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='tv DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['tv'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id='tv_data',
        bash_command='date',
    )

#    t2 = BashOperator(
 #       task_id='sleep',
  #      depends_on_past=False,
   #     bash_command='sleep 5',
    #    retries=3,
   # )

   # t3 = DummyOperator(task_id='t3')
   # t33 = DummyOperator(task_id='t33')
   # t22 = DummyOperator(task_id='t22')
   # task_end = DummyOperator(task_id='end')
   # task_start = DummyOperator(task_id='start')
   # task_empty = DummyOperator(task_id='empty')

   # t1 >> [t2, t3] >> task_end
   # task_start >> t1
   # t3 >> task_empty
   # t1 >> t22 >> task_end
   # t1 >> t33 >> task_end
   # task_empty >> task_end
    #t3 = DummyOperator(task_id='t3')
    age = DummyOperator(task_id='age')
    job = DummyOperator(task_id='job')
    country = DummyOperator(task_id='country')
    sex = DummyOperator(task_id='sex')
    cal = DummyOperator(task_id='cal')
    task_start = DummyOperator(task_id='start')
    task_end = DummyOperator(task_id='end')

    task_start >> t1
    t1 >> age 
    t1 >> job 
    t1 >> country 
    t1 >> sex 
    age >> cal >> task_end
    job >> cal >> task_end
    country >> cal >> task_end
    sex >> cal >> task_end


