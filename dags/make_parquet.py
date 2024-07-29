from datetime import datetime, timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

def gen_emp(id, rule='all_success'):
    op = EmptyOperator(task_id=id, trigger_rule=rule) 
    return op

with DAG(
        'make_parquet',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='make_pqrquet DAG',
    schedule="10 4 * * *",
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['make', 'pqrquet'],
) as dag:


    task_start = gen_emp('start')
    task_end = gen_emp('end', rule='all_done')


    task_check = BashOperator(
        task_id="check.done",
        bash_command="""
            DONE_FILE={{ var.value.IMPORT_DONE_PATH }}/{{ds_nodash}}/_DONE
            bash {{ var.value.CHECK_SH }} $DONE_FILE
            """
    )


    task_to_parquet = BashOperator(
        task_id="to.pqrquet",
        bash_command="""
            echo 'parquet'

            READ_PATH='~/data/csv/{{ds_nodash}}/csv.csv'
            SAVE_PATH='~/data/parquet'
            #mkdir -p $SAVE_PATH
            python /home/sujin/airflow/py/csv2parquet.py $READ_PATH $SAVE_PATH
        """
    )

    task_done = BashOperator(
        task_id="make.done",
        bash_command="""
            echo 'done'
        """
    )

    task_error = BashOperator(
        task_id="error",
        bash_command="""
            echo 'error'
        """
    )




    task_start >> task_check
    task_check >> task_to_parquet >> task_done
    task_done >> task_error >> task_end



