import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, \
    BigQueryCreateExternalTableOperator

# fixed date value for test purpose.


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

delete_sql = '''
DELETE FROM `tmdb-movies-410603.dev.movie_details` d
WHERE d.id IN (
    SELECT id
      FROM `tmdb-movies-410603.dev.changes-{{ ds }}`
)
'''
merge_sql = '''
MERGE `tmdb-movies-410603.dev.movie_details` d
USING `tmdb-movies-410603.dev.changes-{{ ds }}` c
ON d.id = c.id
WHEN NOT MATCHED THEN
  INSERT ROW
'''

with DAG(
        'Change_to_Bigquery',
        default_args=default_args,
        description='A simple DAG to create a new table in BigQuery from TMDB change API by triggering a Cloud Function',
        schedule_interval=None
) as dag:
    fetch_changes = SimpleHttpOperator(
        task_id='merge_test_function',
        method='POST',
        http_conn_id='functions_http',
        endpoint='tmdb-changes',
        headers={"Content-Type": "application/json"},
        data=json.dumps({'date': '{{ ds }}'}),
        extra_options={'timeout': 3600},
        dag=dag,
    )

    create_external_table = BigQueryCreateExternalTableOperator(
        task_id='merge_test_create',
        table_resource={
        "tableReference": {
            "projectId": "tmdb-movies-410603",
            "datasetId": "dev",
            "tableId": "changes-{{ ds }}",
        },
        "externalDataConfiguration": {
            "source_uris": ["gs://tmdb-movies-dl/changes-{{ ds }}.json"],
            "source_format": "NEWLINE_DELIMITED_JSON",
            "autodetect": True,}
        },
        gcp_conn_id='bigquery_dev',
        dag=dag,

    )

    delete_existing_rows = BigQueryExecuteQueryOperator(
        task_id='merge_test_delete',
        sql=merge_sql,
        use_legacy_sql=False,
        gcp_conn_id='bigquery_dev',
        dag=dag,
    )

    merge_changes = BigQueryExecuteQueryOperator(
        task_id='merge_test_merge',
        sql=merge_sql,
        use_legacy_sql=False,
        gcp_conn_id='bigquery_dev',
        dag=dag,
    )

fetch_changes >> create_external_table >> delete_existing_rows >> merge_changes