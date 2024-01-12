from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# Define default_args dictionary to specify the default parameters for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

delete_sql = '''
DELETE FROM `tmdb-movies-410603.dev.movie_details` d
WHERE d.id IN (
    SELECT id
      FROM `tmdb-movies-410603.dev.changes-2024-01-11`
)
'''
merge_sql = '''
MERGE `tmdb-movies-410603.dev.movie_details` d
USING `tmdb-movies-410603.dev.changes-2024-01-11` c
ON d.id = c.id
WHEN NOT MATCHED THEN
  INSERT ROW
'''

with DAG(
    'bigquery_merge_table_test',
    default_args=default_args,
    description='A simple DAG to create a new table in BigQuery from an existing table',
    schedule_interval=None
) as dag:
    delete_existing_rows = BigQueryExecuteQueryOperator(
        task_id='merge_test_delete',
        sql=merge_sql,
        use_legacy_sql=False,
        gcp_conn_id='google_cloud_default',
        dag=dag,
    )

    merge_changes = BigQueryExecuteQueryOperator(
        task_id='merge_test_merge',
        sql=merge_sql,
        use_legacy_sql=False,
        gcp_conn_id='google_cloud_default',
        dag=dag,
    )

delete_existing_rows >> merge_changes
