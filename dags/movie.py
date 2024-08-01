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
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=3,
    description='movie',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['moviei','api','amt'],
) as dag:
    
#    def get_data(ds_nodash):
#        from mov.api.call import get_key,save2df
#        df,url = save2df(ds_nodash)
#        print(df.head(5))
#
#    def fun_multi_y(ds_nodash):
#        from mov.api.call import save2df
#        p = {"multiMovieYn" : "Y"}
#        #df,url = save2df(load_dt=ds_nodash, url_param=p)
#        df = save2df(load_dt=ds_nodash, url_param=p)
#        print(df)
#
#    def fun_multi_n(ds_nodash):
#        from mov.api.call import save2df
#        p = {"multiMovieYn" : "N"}
#        df = save2df(load_dt=ds_nodash, url_param=p)
#        print()
#
#    def fun_nation_k(ds_nodash):
#        from mov.api.call import save2df
#        p = {"repNationCd" : "K"}
#        df = save2df(load_dt=ds_nodash, url_param=p)
#        print(df)
#
#    def fun_nation_f(ds_nodash):
#        from mov.api.call import save2df
#        p = {"repNationCd" : "F"}
#        df = save2df(load_dt=ds_nodash, url_param=p)
#        print(df)
#

    def common_get_data(dt, url_param={}):
        from mov.api.call import save2df
        df = save2df(load_dt=dt, url_param=url_param)
        print(df)
        
        for k,v in url_param.items():
            df[k]=v

        p_cols = ['load_dt'] + list(url_param.keys())
        df.to_parquet('~/tmp/test_parquet', partition_cols=p_cols)
                # partition_cols=['load_dt', 'movieKey']
        
        #print(f"load_date: {load_dt}")
        #print(f"dict: {url_param}")



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
            return "rm.dir" #rmdir.task_id
        else:
            return "get.start", "echo.task"



        

    branch_op = BranchPythonOperator(
            task_id="branch.op",
            python_callable=branch_fun
    )

    #run_this = PythonOperator(
        #task_id="print_the_co 

    get_data = PythonVirtualenvOperator(
            task_id='get.data',
            python_callable=common_get_data,
            requirements=["git+https://github.com/sooj1n/mov.git@0.3/api"],
            system_site_packages=False,
            op_args=["{{ds_nodash}}"]
            #venv_cache_path="/home/sujin/tmp2/air_venv/get_data"
    )
    
    get_start = EmptyOperator(task_id='get.start', trigger_rule='all_done')
    get_end = EmptyOperator(task_id='get.end')

    task_start= gen_emp('start')
    task_end = gen_emp('end', rule='all_done')

    #multi_y = EmptyOperator(task_id='multi.y') # 다양성 영화 유무
    #multi_n = EmptyOperator(task_id='multi.n') 
    #nation_k = EmptyOperator(task_id='nation.k') # 한국영화 
    #nation_f = EmptyOperator(task_id='nation.f') # 외국영화


    throw_err= BashOperator(
            task_id='throw.err',
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
   
    #다양성 영화 유무
    multi_y = PythonVirtualenvOperator(
        task_id="multi.y",
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=["git+https://github.com/sooj1n/mov.git@0.3/api"],
        op_args=["{{ds_nodash}}"],
        op_kwargs={"url_param" : {"multiMovieYn": "Y"}}
    )
    multi_n = PythonVirtualenvOperator(
        task_id="multi.n",
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=["git+https://github.com/sooj1n/mov.git@0.3/api"],
        op_args=["{{ds_nodash}}"],
        op_kwargs={"url_param" : {"multiMovieYn": "N"}}
    )
    #한국영화 
    nation_k = PythonVirtualenvOperator(
        task_id="nation.k",
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=["git+https://github.com/sooj1n/mov.git@0.3/api"],
        op_args=["{{ds_nodash}}"],
        op_kwargs={"url_param" : {"repNationCd": "K"}}
    )

    nation_f = PythonVirtualenvOperator(
        task_id="nation.f",
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=["git+https://github.com/sooj1n/mov.git@0.3/api"],
        op_args=["{{ds_nodash}}"],
        op_kwargs={"url_param" : {"repNationCd": "F"}}
    )

    rm_dir = BashOperator(
            task_id='rm.dir',
            bash_command='rm -rf ~/tmp/test_parquet/load_dt={{ ds_nodash }}'
    )

    echo_task = BashOperator(
        task_id="echo.task",
        bash_command="echo 'task'"
        
    )

    task_start >> branch_op
    task_start >> throw_err >> task_save
    
    rm_dir >> get_start >> [get_data, multi_y, multi_n, nation_k, nation_f] 

    branch_op >> rm_dir 
    #branch_op >> [get_data, multi_y, multi_n, nation_k, nation_f]
    branch_op >> get_start
    branch_op >> echo_task 

    [get_data, multi_y, multi_n, nation_k, nation_f] >> get_end >> task_save >> task_end
