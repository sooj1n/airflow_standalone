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
    'movie',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': True,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='movie',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['moviei','api','amt'],
) as dag:
    def get_data(ds_nodash):
        from mov.api.call import get_key,save2df
        df=save2df(ds_nodash)
        print(df.head(5))


    def save_data(ds_nodash):
        from mov.api.call import apply_type2df
        df = apply_type2df(load_dt=ds_nodash)
        print("*" * 33)
        print(df.head(10))
        print("*" * 33)
        print(df.dtypes)
        
        #개봉일 기준 그룹핑 누적 관객수 합 
        print("개봉일 기준 그룹핑 누적 관객수 합")
        g = df.groupby('openDt')
        sum_df = g.agg({'audiCnt' : 'sum'}).reset_index()
        print(sum_df)

    def print_context(ds=None, **kwargs):
        pprint(kwargs)
        print(ds)


    def branch_fun(ds_nodash):
        import os
        home_dir = os.path.expanduser("~")
        #path = f('{home_dir}/tmp/test_parquet/load_dt={ld}')
        path = os.path.join(home_dir, f"tmp/test_parquet/load_dt={ds_nodash}")
        #if os.path.exists(f'~/tmp/test_parquet/load_dt={ld}'):
        if os.path.exists(path):
            return "rm.dir"
        else:
            return "get.data", "echo.task"



        

    branch_op = BranchPythonOperator(
            task_id="branch.op",
            python_callable=branch_fun
    )

    #run_this = PythonOperator(
    #        task_id="print_the_context",
    #        python_callable=print_context
    #)

    get_data = PythonVirtualenvOperator(
            task_id='get.data',
            python_callable=get_data,
            requirements=["git+https://github.com/sooj1n/mov.git@0.2/api"],
            system_site_packages=False,
            trigger_rule="all_done",
            #venv_cache_path="/home/sujin/tmp2/air_venv/get_data"
    )

    task_start= gen_emp('start')
    task_end = gen_emp('end', rule='all_done')
    task_join= BashOperator(
            task_id='join',
            bash_command='exit 1',
            trigger_rule="all_done"
    )


    task_save = PythonVirtualenvOperator(
        task_id="save.data",
        python_callable=save_data,
        system_site_packages=False,
        trigger_rule="one_success",
        requirements=["git+https://github.com/sooj1n/mov.git@0.3/api"],
        #venv_cache_path="/home/sujin/tmp2/air_venv/get_data"
    )

    rm_dir = BashOperator(
            task_id='rm.dir',
            bash_command='rm -rf ~/tmp/test_parquet/load_dt=={{ ds_nodash }}'
    )

    echo_task = BashOperator(
        task_id="echo.task",
        bash_command="echo 'task'"
        
    )

    task_start >> branch_op
    task_start >> task_join >> task_save
    
    branch_op >> rm_dir >> get_data
    branch_op >> get_data
    branch_op >> echo_task >> task_save


    get_data >> task_save >> task_end
