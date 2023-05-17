# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession


# COMMAND ----------

spark = SparkSession.builder.appName("PySpark Read Parquet").getOrCreate()

# COMMAND ----------

# Reading parquet file
trade_df = spark.read.format('parquet').load('gs://test-gcs-databricks-bucket/trade_train.parquet')

# COMMAND ----------

trade_df.display()

# COMMAND ----------

trade_df.createOrReplaceTempView('TradeTrain')
trade_df.printSchema()
trade_df.show(truncate=False)

# COMMAND ----------

view_name = 'TradeTrain'
query = f"""Select stock_id, time_id, sum(order_count) as total_orders,
sum(price)  as total_traded_price from {view_name} group by stock_id, time_id
having stock_id = 43
order by stock_id, time_id 
"""
query_df = spark.sql(query)

# COMMAND ----------

query_df.show(truncate=False)

# COMMAND ----------


