# importing libraries
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator




default_args ={
    'owner': 'George',
    'start_date': datetime(year =2024, month =11, day =13),
    'email': 'ifigeorgeifi@yahoo.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': None,
    #'retries_delay': None
}

# instatiate the DAG
with DAG(
    'my_first_dag',
    default_args = default_args,
    description = ' this is a demo dag for learning',
    schedule_interval = '0 0 * * 2,3', # Runs every Tuesday and Wednesday at midnight(check contrab guru)
    catchup = False
) as dag:

#define task
    start_task = EmptyOperator(
    task_id = 'start_pipeline'
)

# define task 2
get_time_task = BashOperator(
    task_id = 'timestamp',
    bash_command = 'echo "current date and time is $(date)"'
)

#define task
end_task = EmptyOperator(
    task_id = 'end_pipeline'
)

# set dependencies
start_task >> get_time_task >> end_task
# importing libraries
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator




default_args ={
    'owner': 'George',
    'start_date': datetime(year =2024, month =11, day =13),
    'email': 'ifigeorgeifi@yahoo.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': None,
    #'retries_delay': None
}

# instatiate the DAG
with DAG(
    'my_first_dag',
    default_args = default_args,
    description = ' this is a demo dag for learning',
    schedule_interval = '0 0 * * 2,3', #check contrab guru
    catchup = False
) as dag:

#define task
    start_task = EmptyOperator(
    task_id = 'start_pipeline'
)

# define task 2
extract_task = BashOperator(
    task_id = 'timestamp',
    bash_command = 'echo "current date and time is $(date)"'
)

# loading to staging task

# loading to production task


#define task
end_task = EmptyOperator(
    task_id = 'end_pipeline'
)

# set dependencies
start_task >> get_time_task >> end_task
