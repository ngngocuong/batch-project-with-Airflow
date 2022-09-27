from datetime import datetime, timedelta
from airflow import DAG 
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator
from utils import _local_to_s3,run_redshift_external_query

# Config
BUCKET_NAME = "" # Add your S3 BUCKET 

# DAG defination
default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "wait_for_downstream": True,
    "start_date": datetime(2022,9,28),
    "retries": 2,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    "user_behavior_v1",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    max_active_runs=1,
)

extract_invoice_purchase_data = PostgresOperator(
    dag=dag,
    task_id="extract_invoice_purchase_data",
    sql="./scripts/sql/unload_invoice_purchase.sql",
    postgres_conn_id="postgres_default",
    params={"invoice_purchase": "/temp/invoice_purchase.csv"},
    depends_on_past=True,
    wait_for_downstream=True,
)

invoice_purchase_to_stage_data_lake = PythonOperator(
    dag=dag,
    task_id="invoice_purchase_to_stage_data_lake",
    python_callable=_local_to_s3,
    op_kwargs={
        "file_name": "/opt/airflow/temp/invoice_purchase.csv",
        "key": "stage/invoice_purchase/{{ ds }}/invoice_purchase.csv",
        "bucket_name": BUCKET_NAME,
        "remove_local": "true",
    },
)

create_external_table = PostgresOperator(
    dag=dag,
    task_id="create_external_table",
    sql="scripts/sql/create_external_table.sql",
    postgres_conn_id="redshift",
)

invoice_purchase_stage_data_lake_to_stage_tbl = PythonOperator(
    dag=dag,
    task_id="invoice_purchase_stage_data_lake_to_stage_tbl",
    python_callable=run_redshift_external_query,
    op_kwargs={
        "qry": "alter table spectrum.invoice_purchase add \
            if not exists partition(insert_date='{{ ds }}') \
            location 's3://"
        + BUCKET_NAME
        + "/stage/invoice_purchase/'{{ ds }}'",
    },
)

generate_invoice_behavior_metric = PostgresOperator(
    dag=dag,
    task_id="generate_invoice_behavior_metric",
    sql="scripts/sql/generate_invoice_behavior_metric.sql",
    postgres_conn_id="redshift",
)

(
    extract_invoice_purchase_data 
    >> invoice_purchase_to_stage_data_lake
    >> create_external_table
    >> invoice_purchase_stage_data_lake_to_stage_tbl
    >> generate_invoice_behavior_metric
)