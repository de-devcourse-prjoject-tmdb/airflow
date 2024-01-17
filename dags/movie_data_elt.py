from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime, timedelta

DATASET_NAME = "tmdb-movies-410603.dev"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 15),
    'end_date': datetime(2024, 6, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

numeric_sql = '''
            MERGE INTO dev.movie_numeric_info AS dest
            USING dev.movie_details AS src
            ON dest.id = src.id
            WHEN MATCHED THEN
              UPDATE SET
                dest.title = src.title,
                dest.popularity = src.popularity,
                dest.budget = src.budget,
                dest.revenue = src.revenue,
                dest.release_date = src.release_date,
                dest.vote_average = src.vote_average,
                dest.vote_count = src.vote_count
            WHEN NOT MATCHED THEN
              INSERT (id, title, popularity, budget, revenue, release_date, vote_average, vote_count)
              VALUES (src.id, src.title, src.popularity, src.budget, src.revenue, src.release_date, src.vote_average, src.vote_count);
        '''

genre_sql = '''
            MERGE INTO dev.movie_genre_info AS dest
            USING (
                SELECT DISTINCT
                    src.id,
                    src.title,
                    src.popularity,
                    src.budget,
                    src.revenue,
                    src.release_date,
                    src.vote_average,
                    src.vote_count,
                    src.runtime,
                    all_genres[offset(0)] as genre
                FROM dev.movie_details AS src
                CROSS JOIN UNNEST(src.genres) as all_genres
            ) AS src
            ON dest.id = src.id AND dest.genre = src.genre
            WHEN MATCHED AND src.genre IS NOT NULL THEN
                UPDATE SET
                    dest.title = src.title,
                    dest.popularity = src.popularity,
                    dest.budget = src.budget,
                    dest.revenue = src.revenue,
                    dest.release_date = src.release_date,
                    dest.vote_average = src.vote_average,
                    dest.vote_count = src.vote_count,
                    dest.runtime = src.runtime,
                    dest.genre = src.genre
            WHEN NOT MATCHED AND src.genre IS NOT NULL THEN
                INSERT (
                    id, title, popularity, budget, revenue, release_date, vote_average, vote_count, runtime, genre
                ) VALUES (
                    src.id, src.title, src.popularity, src.budget, src.revenue, src.release_date, src.vote_average, src.vote_count, src.runtime, src.genre
                );
        '''

runtime_sql = '''
    MERGE INTO dev.movie_runtime_info AS dest
    USING dev.movie_details AS src
    ON dest.id = src.id
    WHEN MATCHED THEN
        UPDATE SET
        dest.title = src.title,
        dest.revenue = src.revenue,
        dest.release_date = src.release_date,
        dest.vote_average = src.vote_average,
        dest.vote_count = src.vote_count,
        dest.runtime = src.runtime,
        dest.runtime_category = 
            CASE
                WHEN src.runtime <= 60 THEN 'Under 60min'
                WHEN src.runtime > 60 AND src.runtime <= 120 THEN '60-120min'
                WHEN src.runtime > 120 THEN 'Over 120min'
            END
    WHEN NOT MATCHED THEN
        INSERT (id, title, revenue, release_date, vote_average, vote_count, runtime, runtime_category)
        VALUES (src.id, src.title, src.revenue, src.release_date, src.vote_average, src.vote_count, src.runtime,
            CASE
                WHEN src.runtime <= 60 THEN 'Under 60min'
                WHEN src.runtime > 60 AND src.runtime <= 120 THEN '60-120min'
                WHEN src.runtime > 120 THEN 'Over 120min'
            END);
'''


with DAG('elt_movie_data', 
        default_args = default_args, 
        description = 'DAG to update numeric, genre, runtime tables',
        schedule_interval = '@daily') as dag:
    
    upsert_numeric_table = BigQueryExecuteQueryOperator(
        task_id = "upsert_numeric_table",
        sql = numeric_sql,
        use_legacy_sql = False,  # Use standard SQL syntax
        gcp_conn_id = 'bigquery_dev',
        location = 'us-west1'
    )

    upsert_genre_table = BigQueryExecuteQueryOperator(
        task_id = "upsert_genre_table",
        sql = genre_sql,
        use_legacy_sql = False,  # Use standard SQL syntax
        gcp_conn_id = 'bigquery_dev',
        location = 'us-west1'
    )

    upsert_runtime_table = BigQueryExecuteQueryOperator(
        task_id = "upsert_runtime_table",
        sql = runtime_sql,
        use_legacy_sql = False,  # Use standard SQL syntax
        gcp_conn_id = 'bigquery_dev',
        location = 'us-west1'
    )

    upsert_numeric_table >> upsert_genre_table >> upsert_runtime_table