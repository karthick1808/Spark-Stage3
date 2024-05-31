# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

# Create SparkSession
spark = SparkSession.builder.master("local").appName("JoinsExample").getOrCreate()

# COMMAND ----------

# Create example data
data1 = [Row(id=1, value="A"), Row(id=2, value="B"), Row(id=3, value="C")]
data2 = [Row(id=1, value="X"), Row(id=2, value="Y"), Row(id=4, value="Z")]

# COMMAND ----------

# Create DataFrames
df1 = spark.createDataFrame(data1)
df2 = spark.createDataFrame(data2)

# COMMAND ----------

# Show DataFrames
print("DataFrame 1")
df1.show()

# COMMAND ----------

print("DataFrame 2")
df2.show()

# COMMAND ----------

#Inner Join
inner_join_df = df1.join(df2, df1.id == df2.id, "inner")
print("Inner Join")
inner_join_df.show()

# COMMAND ----------

#Full Outer Join
full_outer_join_df = df1.join(df2, df1.id == df2.id, "outer")
print("Full Outer Join")
full_outer_join_df.show()

# COMMAND ----------

#left Outer Join
left_outer_join_df = df1.join(df2, df1.id == df2.id, "left_outer")
print("Left Outer Join")
left_outer_join_df.show()


# COMMAND ----------

#Right Outer Join
right_outer_join_df = df1.join(df2, df1.id == df2.id, "right_outer")
print("Right Outer Join")
right_outer_join_df.show()


# COMMAND ----------

#Left Anti Join
left_anti_join_df = df1.join(df2, df1.id == df2.id, "left_anti")
print("Left Anti Join")
left_anti_join_df.show()

# COMMAND ----------

#Left Semi Join
left_semi_join_df = df1.join(df2, df1.id == df2.id, "left_semi")
print("Left Semi Join")
left_semi_join_df.show()


# COMMAND ----------


