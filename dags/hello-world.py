from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="simple_etl_pipeline",
    start_date=datetime(2023, 8, 15),
    schedule="@once",
    tags=["example", "etl"]
) as dag:
    # Extract task: Simulate extracting data by creating a sample file
    extract_task = BashOperator(
        task_id="extract_data",
        bash_command="echo 'name,age,city\nAlice,30,New York\nBob,25,Los Angeles' > /tmp/data.csv"
    )

    # Transform task: Simulate transforming data by filtering and saving to a new file
    transform_task = BashOperator(
        task_id="transform_data",
        bash_command="awk -F',' '$2 > 25 || NR==1' /tmp/data.csv > /tmp/transformed_data.csv"
    )

    # Load task: Simulate loading data by displaying the transformed data
    load_task = BashOperator(
        task_id="load_data",
        bash_command="cat /tmp/transformed_data.csv"
    )

    # Set dependencies: Extract -> Transform -> Load
    extract_task >> transform_task >> load_task
