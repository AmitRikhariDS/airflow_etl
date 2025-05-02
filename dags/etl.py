from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.utils.dates import days_ago
import json

with DAG(
    dag_id='nasa_apod_postgres',
    # start_date=days_ago(1),
    schedule='@daily',
    catchup=False,
    tags=['nasa', 'postgres', 'api'],
) as dag:

    # Step 1: Create the table if it doesn't exist
    @task
    def create_table():
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")
        create_query = """
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
        );
        """
        postgres_hook.run(create_query)

    # Step 2: Extract NASA API Data (APOD)
    extract_apod = HttpOperator(
        task_id="extract_apod",
        http_conn_id='nasa_api',  # Must be defined in Airflow UI
        endpoint='planetary/apod',
        method='GET',
        data={"api_key": "{{ conn.nasa_api.extra_dejson.api_key }}"},
        response_filter=lambda response: response.json(),
        log_response=True,
        do_xcom_push=True
    )

    # Step 3: Transform JSON to dict
    @task
    def transform_apod_data(response: dict):
        return {
            'title': response.get('title', ''),
            'explanation': response.get('explanation', ''),
            'url': response.get('url', ''),
            'date': response.get('date', ''),
            'media_type': response.get('media_type', '')
        }

    # Step 4: Load into PostgreSQL
    @task
    def load_data_to_postgres(apod_data: dict):
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")
        insert_query = """
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s);
        """
        postgres_hook.run(insert_query, parameters=(
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type']
        ))

    # DAG Task Flow
    create = create_table()
    extracted_data = extract_apod
    transformed_data = transform_apod_data(extracted_data.output)
    load_data_to_postgres(transformed_data)
