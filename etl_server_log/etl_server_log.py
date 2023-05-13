# import the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago
#defining DAG arguments
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Waleed Ali',
    'start_date': days_ago(0),
    'email': ['waleed@somemail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
# defining the DAG
# define the DAG
dag = DAG(
    'ETL-Server-Access-Log-Processing',
    default_args=default_args,
    description='ETL to process the server accrss logs',
    schedule_interval=timedelta(days=1),
)
# define the tasks
# define the first task
# downloads the server log data
download = BashOperator(
    task_id='download',
    bash_command='wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt',
    dag=dag,
)

# define the second task
# extracts the relevant fields from the log default_args
extract = BashOperator(
    task_id='extract',
    bash_command='cut -d"#" -f1,4 web-server-access-log.txt > extracted-log.txt',
    dag=dag,
)

# third tasks
# Transforms the visitorid column
transform= BashOperator(
    task_id='transform',
    bash_command='tr  "[:lower:]" "[:upper:]"  < extracted-log.txt > transformed.txt',
    dag=dag,
)

# Fourth task
# Compress transformed data
load = BashOperator(
    task_id='load',
    bash_command='zip ./compress-log.zip transformed.txt',
    dag=dag,
)

# task pipeline
download >> extract >> transform >> load
