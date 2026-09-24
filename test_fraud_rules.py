from pyspark.sql import SparkSession
from fraud_rules import apply_fraud_rules


spark = (
    SparkSession.builder
    .appName("FraudRulesTest")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# Test transactions
data = [
    ("TXN001", 10000.0),
    ("TXN002", 30000.0),
    ("TXN003", 75000.0),
]

columns = ["transaction_id", "amount"]

df = spark.createDataFrame(data, columns)


# Apply fraud detection rules
result = apply_fraud_rules(df)


# Display results
result.show(truncate=False)


# Basic validation
rows = result.collect()

assert rows[0]["fraud_status"] == "NORMAL"
assert rows[1]["fraud_status"] == "SUSPICIOUS"
assert rows[2]["fraud_status"] == "FRAUD"

assert rows[0]["risk_score"] == 10
assert rows[1]["risk_score"] == 50
assert rows[2]["risk_score"] == 90

print("======================================")
print(" FRAUD RULE TEST PASSED SUCCESSFULLY")
print("======================================")


spark.stop()