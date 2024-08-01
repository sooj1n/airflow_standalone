from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.empty import EmptyOperator

from airflow.operators.python import (
        PythonOperator, 
        PythonVirtualenvOperator,
        BranchPythonOperator
)

def gen_emp(id, rule='all_success'):
    op = EmptyOperator(task_id=id, trigger_rule=rule)
    return op

with DAG(
    'movie_summary',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=3,
    description='movie_summary',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['moviei','summary','api','amt'],
) as dag:
    
    task_start = gen_emp('start')
    task_end = gen_emp('end', rule='all_done')

    apply_type = EmptyOperator(
        task_id='apply.type'
    )

    merge_df = EmptyOperator(
        task_id='merge.df'
    )

    de_dup = EmptyOperator(
        task_id='de.dup'
    )

    summary_df = EmptyOperator(
        task_id='summary.df'
    )

    task_start >> apply_type >> merge_df >> de_dup >> summary_df >> task_end




