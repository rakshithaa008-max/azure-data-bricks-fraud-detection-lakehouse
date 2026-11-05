# Databricks notebook source
print("Fraud Detection Project - Databricks Ready!")

# COMMAND ----------

data = [
    (1001, 250.00, "London"),
    (1002, 1500.00, "Manchester"),
    (1003, 75.50, "Birmingham")
]

columns = ["transaction_id", "amount", "city"]

df = spark.createDataFrame(data, columns)

display(df)

# COMMAND ----------

display(spark.sql("SHOW CATALOGS"))

# COMMAND ----------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS dbw_fraud_dev.fraud_detection
""")

print("Fraud detection schema created successfully!")


# COMMAND ----------

df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "dbw_fraud_dev.fraud_detection.bronze_transactions"
    )

display(
    spark.table(
        "dbw_fraud_dev.fraud_detection.bronze_transactions"
    )
)

# COMMAND ----------

try:
    import azure.eventhub
    print("Event Hubs SDK available!")
except ImportError:
    print("Event Hubs SDK not installed")

# COMMAND ----------

# MAGIC %pip install azure-eventhub

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from azure.eventhub import EventHubConsumerClient

print("SUCCESS: Event Hubs SDK ready!")

# COMMAND ----------

display(
    spark.table(
        "dbw_fraud_dev.fraud_detection.bronze_transactions"
    )
)

# COMMAND ----------

bronze_df = spark.table(
    "dbw_fraud_dev.fraud_detection.bronze_transactions"
)

print("Total Bronze records:", bronze_df.count())
display(bronze_df)

# COMMAND ----------

from pyspark.sql.functions import col

silver_df = bronze_df.filter(
    col("transaction_id").isNotNull() &
    col("amount").isNotNull() &
    (col("amount") > 0) &
    col("city").isNotNull()
).dropDuplicates(["transaction_id"])

print("Clean Silver records:", silver_df.count())

display(silver_df)

# COMMAND ----------

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "dbw_fraud_dev.fraud_detection.silver_transactions"
    )

print("SUCCESS: Silver Delta table created!")

# COMMAND ----------

result = spark.sql("""
    SELECT *
    FROM dbw_fraud_dev.fraud_detection.silver_transactions
    ORDER BY transaction_id
""")

display(result)

# COMMAND ----------

from pyspark.sql.functions import col, when

fraud_df = silver_df.withColumn(
    "fraud_flag",
    when(
        col("amount") > 1000,
        "HIGH_VALUE_REVIEW"
    ).otherwise("NORMAL")
)

display(fraud_df)

# COMMAND ----------

fraud_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "dbw_fraud_dev.fraud_detection.gold_transactions"
    )

print("SUCCESS: Gold Delta table created!")

# COMMAND ----------

gold_df = spark.table(
    "dbw_fraud_dev.fraud_detection.gold_transactions"
)

print("Total Gold records:", gold_df.count())

display(gold_df)

# COMMAND ----------

summary_df = spark.sql("""
SELECT
    fraud_flag,
    COUNT(*) AS total_transactions,
    SUM(amount) AS total_amount
FROM dbw_fraud_dev.fraud_detection.gold_transactions
GROUP BY fraud_flag
ORDER BY fraud_flag
""")

display(summary_df)

# COMMAND ----------

summary_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "dbw_fraud_dev.fraud_detection.gold_fraud_summary"
    )

print("SUCCESS: Gold fraud summary table created!")

# COMMAND ----------

saved_summary_df = spark.table(
    "dbw_fraud_dev.fraud_detection.gold_fraud_summary"
)

print("Total summary rows:", saved_summary_df.count())

display(saved_summary_df)

# COMMAND ----------

kpi_df = spark.sql("""
SELECT
    SUM(total_transactions) AS total_transactions,
    SUM(
        CASE
            WHEN fraud_flag = 'HIGH_VALUE_REVIEW'
            THEN total_transactions
            ELSE 0
        END
    ) AS flagged_transactions,
    ROUND(
        100.0 * SUM(
            CASE
                WHEN fraud_flag = 'HIGH_VALUE_REVIEW'
                THEN total_transactions
                ELSE 0
            END
        ) / NULLIF(SUM(total_transactions), 0),
        2
    ) AS review_rate_pct
FROM dbw_fraud_dev.fraud_detection.gold_fraud_summary
""")

display(kpi_df)

# COMMAND ----------

kpi_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "dbw_fraud_dev.fraud_detection.gold_fraud_kpi"
    )

print("SUCCESS: Gold fraud KPI table created!")

# COMMAND ----------

kpi_check_df = spark.table(
    "dbw_fraud_dev.fraud_detection.gold_fraud_kpi"
)

display(kpi_check_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     total_transactions,
# MAGIC     flagged_transactions,
# MAGIC     review_rate_pct
# MAGIC FROM dbw_fraud_dev.fraud_detection.gold_fraud_kpi;