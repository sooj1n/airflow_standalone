from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.empty import EmptyOperator

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

def gen_emp(id, rule='all_success'):
    op = EmptyOperator(task_id=id, trigger_rule=rule)
    return op

with DAG(
    'movie',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': True,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='movie',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['moviei','api','amt'],
) as dag:


    task_start = gen_emp('start')
    task_end = gen_emp('end', rule='all_done')

    task_get = BashOperator(
        task_id="get.data",
        bash_command="""
            echo 'get'
        """
    )


    task_save = BashOperator(
        task_id="save.data",
        bash_command="""
            echo 'save'
        """
    )


    task_start >> task_get >> task_save >> task_end
