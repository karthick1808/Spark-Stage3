# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import Row

# COMMAND ----------

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PivotUnpivotExample") \
    .getOrCreate()

# COMMAND ----------

# Sample data
data = [
    Row(name="Alice", subject="Math", score=85),
    Row(name="Alice", subject="English", score=78),
    Row(name="Alice", subject="Science", score=92),
    Row(name="Bob", subject="Math", score=89),
    Row(name="Bob", subject="English", score=76),
    Row(name="Bob", subject="Science", score=94)
]

# COMMAND ----------

# Create DataFrame
df = spark.createDataFrame(data)
df.show()

# COMMAND ----------

# Pivot the DataFrame
pivot_df = df.groupBy("name").pivot("subject").sum("score")
pivot_df.show()

# COMMAND ----------

# Sample data with duplicate entries
data = [
    ("Alice", "Math", 85),
    ("Alice", "Math", 15),
    ("Alice", "English", 78),
    ("Alice", "English", 22),
    ("Alice", "Science", 92),
    ("Bob", "Math", 89),
    ("Bob", "Math", 11),
    ("Bob", "English", 76),
    ("Bob", "Science", 94),
    ("Bob", "Science", 6)
]
# Create DataFrame
df = spark.createDataFrame(data, ["name", "subject", "score"])
print("Original DataFrame:")
df.show()

# COMMAND ----------

# Pivot the DataFrame
pivot_df = df.groupBy("name").pivot("subject").avg("score")
print("After Pivot with Sum:")
pivot_df.show()

# COMMAND ----------

# sum: Calculates the sum of the values in each group.
# avg: Calculates the average of the values in each group.
# max: Finds the maximum value in each group.
# min: Finds the minimum value in each group.
# count: Counts the number of non-null values in each group.
# countDistinct: Counts the number of distinct values in each group.
# Pivot with different aggregation functions
from pyspark.sql.functions import sum, avg, max, min, count, countDistinct
pivot_df_sum = df.groupBy("name").pivot("subject").sum("score")
pivot_df_avg = df.groupBy("name").pivot("subject").avg("score")
pivot_df_max = df.groupBy("name").pivot("subject").max("score")
pivot_df_min = df.groupBy("name").pivot("subject").min("score")
pivot_df_count = df.groupBy("name").pivot("subject").agg(count("score"))
pivot_df_countDistinct = df.groupBy("name").pivot("subject").agg(countDistinct("score"))

# Show results
print("Pivot with Sum:")
pivot_df_sum.show()
print("Pivot with Avg:")
pivot_df_avg.show()
print("Pivot with Max:")
pivot_df_max.show()
print("Pivot with Min:")
pivot_df_min.show()
print("Pivot with Count:")
pivot_df_count.show()
print("Pivot with CountDistinct:")
pivot_df_countDistinct.show()

# COMMAND ----------

unpivot_df = pivot_df.selectExpr("name","stack(3, 'NMath', Math, 'NEnglish', English, 'NScience', Science) as (Nsubject, Nscore)")
unpivot_df.show()

# COMMAND ----------

unpivot_df1 = pivot_df.selectExpr("name","stack(3, 'NMath', Math, 'NEnglish', English, 'NScience', Science) as (Nsubject, Nscore)").where("NScore is not null")
unpivot_df1.show()

# COMMAND ----------


