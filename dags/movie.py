from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.empty import EmptyOperator

from airflow.operators.python import PythonOperator

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
    def get_data(ds, **kwargs):
        print(ds)
        print(kwargs)
        print(f"ds_nodash =>{kwargs['ds_nodash']}")
        print(f"kwargs type => {type(kwargs)}")
        from mov.api.call import get_key,save2df
        key=get_key()
        print(f"MOVIE_API_KEY => {key}")
        YYYYMMDD=kwargs['ds_nodash']
        df=save2df(YYYYMMDD)
        print(df.head(5))


    def print_context(ds=None, **kwargs):
        pprint(kwargs)
        print(ds)

    run_this = PythonOperator(
            task_id="print_the_context",
            python_callable=print_context
    )

    get_data = PythonOperator(
            task_id='get_data',
            python_callable=get_data
    )

    task_start= gen_emp('start')
    task_end = gen_emp('end', rule='all_done')


    task_save = BashOperator(
        task_id="save.data",
        bash_command="""
            echo 'save'
        """
    )


    task_start >> get_data >> task_save >> task_end
    task_start >> run_this >> task_end
