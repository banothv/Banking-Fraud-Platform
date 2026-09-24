import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)
POSTGRES_URL = "jdbc:postgresql://localhost:5432/ecommerce_db"

POSTGRES_PROPERTIES = {
    "user": "ecommerce_user",
    "password": os.getenv("POSTGRES_PASSWORD"),
    "driver": "org.postgresql.Driver"
}

from fraud_rules import apply_fraud_rules

def write_to_postgres(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    import psycopg2
    from psycopg2.extras import execute_values

    rows = batch_df.select(
        "transaction_id",
        "customer_id",
        "amount",
        "location",
        "risk_score",
        "fraud_status",
        "fraud_reason"
    ).collect()

    if not rows:
        return

    conn = None

    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="ecommerce_db",
            user="ecommerce_user",
            password=os.getenv("POSTGRES_PASSWORD")
        )

        cursor = conn.cursor()

        # --------------------------------------------------
        # 1. Insert transactions
        # Duplicate transaction IDs are ignored
        # --------------------------------------------------

        transaction_sql = """
            INSERT INTO transactions (
                transaction_id,
                customer_id,
                amount,
                location,
                risk_score,
                fraud_status,
                fraud_reason
            )
            VALUES %s
            ON CONFLICT (transaction_id) DO NOTHING
        """

        transaction_values = [
            (
                row["transaction_id"],
                row["customer_id"],
                row["amount"],
                row["location"],
                row["risk_score"],
                row["fraud_status"],
                row["fraud_reason"]
            )
            for row in rows
        ]

        execute_values(
            cursor,
            transaction_sql,
            transaction_values
        )

        # --------------------------------------------------
        # 2. Insert fraud alerts
        # --------------------------------------------------

        alert_rows = [
            (
                row["transaction_id"],
                row["customer_id"],
                row["risk_score"],
                row["fraud_status"],
                row["fraud_reason"]
            )
            for row in rows
            if row["fraud_status"] in ("FRAUD", "SUSPICIOUS")
        ]

        if alert_rows:

            alert_sql = """
                INSERT INTO fraud_alerts (
                    transaction_id,
                    customer_id,
                    risk_score,
                    fraud_status,
                    fraud_reason
                )
                SELECT *
                FROM (VALUES %s) AS new_alerts(
                    transaction_id,
                    customer_id,
                    risk_score,
                    fraud_status,
                    fraud_reason
                )
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM fraud_alerts existing
                    WHERE existing.transaction_id = new_alerts.transaction_id
                )
            """

            execute_values(
                cursor,
                alert_sql,
                alert_rows
            )

        conn.commit()

        print(
            f"Batch {batch_id}: "
            f"{len(rows)} transactions processed successfully",
            flush=True
        )

        if alert_rows:
            print(
                f"Batch {batch_id}: "
                f"{len(alert_rows)} fraud/suspicious alerts processed",
                flush=True
            )

    except Exception as e:
        if conn:
            conn.rollback()

        print(
            f"Batch {batch_id}: PostgreSQL error: {e}",
            flush=True
        )

        raise

    finally:
        if conn:
            conn.close()

def main():

    print("PYTHON STREAMING SCRIPT STARTED", flush=True)

    spark = (
    SparkSession.builder
    .appName("BankingFraudStreaming")
    .master("local[*]")
    .config("spark.driver.extraJavaOptions", "-Duser.timezone=Asia/Kolkata")
    .config("spark.executor.extraJavaOptions", "-Duser.timezone=Asia/Kolkata")
    .config(
        "spark.jars",
        "spark/jars/postgresql-42.7.8.jar"
    )
    .getOrCreate()
)
    spark.sparkContext.setLogLevel("WARN")

    print("======================================", flush=True)
    print(" BANKING FRAUD STREAMING STARTED", flush=True)
    print("======================================", flush=True)

    # Kafka transaction schema
    transaction_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("location", StringType(), True),
    ])

    # Read transactions from Kafka
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "bank-transactions")
        .option("startingOffsets", "latest")
        .load()
    )

    # Convert Kafka value from binary to JSON string
    json_df = kafka_df.selectExpr(
        "CAST(value AS STRING) AS json_value"
    )

    # Convert JSON into structured columns
    transactions = (
        json_df
        .select(
            from_json(
                col("json_value"),
                transaction_schema
            ).alias("data")
        )
        .select("data.*")
    )

    # Apply fraud detection rules
    fraud_results = apply_fraud_rules(transactions)

    # Display fraud detection results
    query = (
    fraud_results.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .option("checkpointLocation", "spark/checkpoints/fraud_postgres")
    .start()
)
    print("======================================", flush=True)
    print(" CONNECTED TO KAFKA", flush=True)
    print(" TOPIC: bank-transactions", flush=True)
    print(" FRAUD DETECTION: ENABLED", flush=True)
    print("======================================", flush=True)

    query.awaitTermination()


if __name__ == "__main__":
    main()
    POSTGRES_URL = "jdbc:postgresql://localhost:5432/ecommerce_db"
POSTGRES_PROPERTIES = {
    "user": "ecommerce_user",
    "password": os.getenv("POSTGRES_PASSWORD"),
    "driver": "org.postgresql.Driver"
}