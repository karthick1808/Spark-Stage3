# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark import StorageLevel

# COMMAND ----------

# Initialize Spark session
spark = SparkSession.builder.appName("CacheAndPersistExample").getOrCreate()


# COMMAND ----------

# Create a sample DataFrame
data = [("Alice", 34), ("Bob", 45), ("Catherine", 29), ("David", 54)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, schema=columns)

# COMMAND ----------

# Show the original DataFrame
print("Original DataFrame:")
df.show()

# COMMAND ----------

# Cache the DataFrame
df.cache()
print("DataFrame cached in MEMORY_ONLY (default):")
df.show()

# COMMAND ----------

# Unpersist the DataFrame to remove from cache
df.unpersist()

# COMMAND ----------

# Persist the DataFrame in MEMORY_ONLY storage level
df.persist(StorageLevel.MEMORY_ONLY)
print("DataFrame persisted in MEMORY_ONLY:")
df.show()
df.unpersist()

# COMMAND ----------

# Persist the DataFrame in MEMORY_AND_DISK storage level
df.persist(StorageLevel.MEMORY_AND_DISK)
print("DataFrame persisted in MEMORY_AND_DISK:")
df.show()
df.unpersist()

# COMMAND ----------

# Persist the DataFrame in DISK_ONLY storage level
df.persist(StorageLevel.DISK_ONLY)
print("DataFrame persisted in DISK_ONLY:")
df.show()
df.unpersist()

# COMMAND ----------

# Persist the DataFrame in MEMORY_ONLY_SER storage level
df.persist(StorageLevel.MEMORY_ONLY_SER)
print("DataFrame persisted in MEMORY_ONLY_SER (serialized):")
df.show()
df.unpersist()

# COMMAND ----------

# Persist the DataFrame in MEMORY_AND_DISK_SER storage level
df.persist(StorageLevel.MEMORY_AND_DISK_SER)
print("DataFrame persisted in MEMORY_AND_DISK_SER (serialized):")
df.show()
df.unpersist()


# COMMAND ----------


