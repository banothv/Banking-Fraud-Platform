from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    when,
    lit,
)


def apply_fraud_rules(df: DataFrame) -> DataFrame:
    """
    Apply fraud detection rules to banking transactions.

    Rules:
    1. High-value transaction: amount > 50,000
    2. Medium-risk transaction: amount between 20,000 and 50,000
    3. Normal transaction: amount <= 20,000

    Returns the DataFrame with:
    - risk_score
    - fraud_status
    - fraud_reason
    """

    result = (
        df
        .withColumn(
            "risk_score",
            when(col("amount") > 50000, lit(90))
            .when(col("amount") > 20000, lit(50))
            .otherwise(lit(10))
        )
        .withColumn(
            "fraud_status",
            when(col("amount") > 50000, lit("FRAUD"))
            .when(col("amount") > 20000, lit("SUSPICIOUS"))
            .otherwise(lit("NORMAL"))
        )
        .withColumn(
            "fraud_reason",
            when(
                col("amount") > 50000,
                lit("High-value transaction")
            )
            .when(
                col("amount") > 20000,
                lit("Medium-value transaction")
            )
            .otherwise(
                lit("Normal transaction")
            )
        )
    )

    return result