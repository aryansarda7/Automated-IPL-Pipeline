from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from get_ipl_matches_auto import get_ipl_matches
from main_pipeline import run_full_pipeline
from update_mysql_tables import update_mysql_tables
from airflow_refresh import refresh_superset_charts

default_args = {
    'owner' : '   ',
    'depends_on_past' : False,
    'start_date' : datetime(2025, 5, 16),
    'email' : ['   '],
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=1)
}

dag = DAG(
    'ipl_pipeline_dag',
    default_args = default_args,
    description = 'IPL ETL Pipeline',
    schedule_interval='0 15 * * *',  # Runs at 15:00 (3 PM) UTC every day
    catchup=False,
    tags=['IPL', 'cricket'],
    max_active_runs=1
)

t1 = PythonOperator(
    task_id = 'fetch_ipl_matches_json',
    python_callable = get_ipl_matches,
    dag = dag,
)

t2 = PythonOperator(
    task_id = 'run_main_pipeline',
    python_callable = run_full_pipeline,
    dag = dag,
    provide_context=True
)

t3 = PythonOperator(
    task_id = 'update_sql_tables',
    python_callable = update_mysql_tables,
    dag = dag,
)

t4 = PythonOperator(
    task_id = 'refresh_superset',
    python_callable = refresh_superset_charts,
    dag = dag,
)

# Add task dependencies at the end
t1 >> t2 >> t3 >> t4
