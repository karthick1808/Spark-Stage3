# Databricks notebook source
# Rule-Based Optimizer (Catalyst): Found in all versions of Apache Spark.
# Cost-Based Optimizer: Introduced as an experimental feature in Apache Spark 2.2.0.
# Adaptive Query Execution: Introduced as an experimental feature in Apache Spark 2.3.0 and became stable in Spark 3.0.0.

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", "true")

# COMMAND ----------

spark.conf.get("spark.sql.adaptive.enabled")

# COMMAND ----------


