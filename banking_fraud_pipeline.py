from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


def validate_pipeline():
    print("Validating banking fraud pipeline...")
    print("Kafka → PySpark → PostgreSQL → Dashboard")


def check_postgres():
    print("Checking PostgreSQL banking tables...")
    print("transactions")
    print("fraud_alerts")


def generate_summary():
    print("Generating daily banking fraud summary...")


def verify_alerts():
    print("Verifying fraud and suspicious alerts...")


with DAG(
    dag_id="banking_fraud_pipeline",
    start_date=datetime(2026, 9, 1),
    schedule=timedelta(days=1),
    catchup=False,
    tags=["banking", "fraud", "data-engineering"],
) as dag:

    validate = PythonOperator(
        task_id="validate_pipeline",
        python_callable=validate_pipeline,
    )

    postgres = PythonOperator(
        task_id="check_postgres",
        python_callable=check_postgres,
    )

    summary = PythonOperator(
        task_id="generate_daily_summary",
        python_callable=generate_summary,
    )

    alerts = PythonOperator(
        task_id="verify_fraud_alerts",
        python_callable=verify_alerts,
    )

    validate >> postgres >> summary >> alerts