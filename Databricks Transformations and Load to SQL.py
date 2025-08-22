# Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.bootcampstoragexyz.dfs.core.windows.net",
  ##"insert storage account key here"
)

# COMMAND ----------

path = "abfss://filesystem@bootcampstoragexyz.dfs.core.windows.net/raw/Transactions.csv"

df_transactions = (
    spark.read
         .format("csv")
         .option("header", "true")
         .option("sep", ",")         
         .option("inferSchema", "true")
         .load(path)
)
display(df_transactions)



# COMMAND ----------

spark.read.text(path).show(5, truncate=False)
import requests

token_resp = requests.get(
    "http://169.254.169.254/metadata/identity/oauth2/token",
    params={
        "api-version": "2018-02-01",
        "resource": "https://database.windows.net/"
    },
    headers={"Metadata": "true"}
).json()

access_token = token_resp["access_token"]


# COMMAND ----------

jdbc_url = (
    "jdbc:sqlserver://avanadebootcamp.database.windows.net:1433;"
    "database=debootcamp;"
    "encrypt=true;"
    "hostNameInCertificate=*.database.windows.net;"
    "authentication=ActiveDirectoryMSI;"
)

df_rules = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "dbo.Rules") \
    .load()


# COMMAND ----------

display(df_rules)

# COMMAND ----------

from pyspark.sql import functions as F

df_flagged = (
    df_transactions
    .withColumn("rule_1", (F.col("amount") > 10000) & (F.hour("txn_time") > 22))
    .withColumn("rule_2", F.col("txn_location") != F.col("customer_home_location"))
    .withColumn("rule_3", F.col("txn_count_1min") > 3)
)

df_flagged = df_flagged.withColumn(
    "is_flagged", F.col("rule_1") | F.col("rule_2") | F.col("rule_3")
)

transactions_flagged = df_flagged.filter(F.col("is_flagged") == True)
transactions_cleaned = df_flagged.filter(F.col("is_flagged") == False)

fraud_summary_by_day = (
    transactions_flagged
    .withColumn("txn_date", F.to_date("txn_time"))
    .groupBy("txn_date")
    .agg(F.count("*").alias("flagged_count"))
    .orderBy("txn_date")
)

display(transactions_cleaned)
display(transactions_flagged)
display(fraud_summary_by_day)


# COMMAND ----------

# Write transactions_cleaned
transactions_cleaned.write.mode("overwrite").parquet(
    "abfss://filesystem@bootcampstoragexyz.dfs.core.windows.net/curated/transactions_cleaned"
)

# Write transactions_flagged
transactions_flagged.write.mode("overwrite").parquet(
    "abfss://filesystem@bootcampstoragexyz.dfs.core.windows.net/curated/transactions_flagged"
)

# Write fraud_summary_by_day
fraud_summary_by_day.write.mode("overwrite").parquet(
    "abfss://filesystem@bootcampstoragexyz.dfs.core.windows.net/curated/fraud_summary_by_day"
)


# COMMAND ----------

df_cleaned = spark.read.parquet("abfss://filesystem@bootcampstoragexyz.dfs.core.windows.net/curated/transactions_cleaned")
df_flagged = spark.read.parquet("abfss://filesystem@bootcampstoragexyz.dfs.core.windows.net/curated/transactions_flagged")
df_summary = spark.read.parquet("abfss://filesystem@bootcampstoragexyz.dfs.core.windows.net/curated/fraud_summary_by_day")


# COMMAND ----------

sql_server_name = "avanadebootcamp.database.windows.net"   # no https://
sql_database_name = "bootcampdb"
sql_table_cleaned = "transactions_cleaned"
sql_table_flagged = "transactions_flagged"
sql_table_summary = "fraud_summary_by_day"

# Managed Identity version
jdbc_url = f"jdbc:sqlserver://{sql_server_name}:1433;database={sql_database_name};encrypt=true;hostNameInCertificate=*.database.windows.net;authentication=ActiveDirectoryMSI;"


# COMMAND ----------

import requests

token = requests.get(
    "http://169.254.169.254/metadata/identity/oauth2/token",
    params={
        "resource": "https://database.windows.net/",
        "api-version": "2018-02-01"
    },
    headers={"Metadata": "true"}
).json()["access_token"]

# COMMAND ----------

jdbc_url = (
    "jdbc:sqlserver://avanadebootcamp.database.windows.net:1433;"
    "database=bootcampdb;"
    "encrypt=true;"
    "trustServerCertificate=false;"
    "hostNameInCertificate=*.database.windows.net;"
    "Authentication=ActiveDirectoryMSI;"
)

# COMMAND ----------

# transactions_cleaned
df_cleaned.write \
    .format("jdbc") \
    .mode("overwrite") \
    .option("url", jdbc_url) \
    .option("dbtable", sql_table_cleaned) \
    .option("accessToken", token) \
    .save()

# transactions_flagged
df_flagged.write \
    .format("jdbc") \
    .mode("overwrite") \
    .option("url", jdbc_url) \
    .option("dbtable", sql_table_flagged) \
    .save()

# fraud_summary_by_day
df_summary.write \
    .format("jdbc") \
    .mode("overwrite") \
    .option("url", jdbc_url) \
    .option("dbtable", sql_table_summary) \
    .save()
