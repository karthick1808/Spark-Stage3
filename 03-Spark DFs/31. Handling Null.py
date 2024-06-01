# Databricks notebook source
# isNull() / isNotNull(): These methods are used to filter DataFrame rows based on whether a column contains null values or not.
# na.drop(): This method drops rows containing any null or NaN values in the DataFrame.
# na.fill(): This method fills null or NaN values in the DataFrame with specified values.
# fillna(): This method fills null values in DataFrame columns with a specified value.
# coalesce(): This method replaces null values with non-null values from other columns.
# dropna(): This method drops rows containing any null values in the specified columns.
# fillna(): This function fills null values in DataFrame columns with a specified value.
# replace(): This function replaces specific values with another value.
# when() / otherwise(): These functions can be used for conditional replacement of null values.
# drop(): This method drops columns containing null values.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# Define data
data_student = [
    ("Michael", "Science", 80, "P", 90),
    ("Nancy", "Mathematics", 90, "P", None),
    ("David", "English", 20, "F", 80),
    ("John", "Science", None, "F", None),
    ("Blessy", None, 30, "F", 50),
    ("Martin", "Mathematics", None, None, 70),
    (None,None,None,None,None)
]

# Define schema
schema = StructType([
    StructField("name", StringType(), True),
    StructField("Subject", StringType(), True),
    StructField("Mark", IntegerType(), True),
    StructField("Status", StringType(), True),
    StructField("Attendance", IntegerType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data=data_student, schema=schema)

# Display DataFrame
display(df)

# COMMAND ----------

from pyspark.sql.functions import col
# Filter rows where column 'col' is null
# display(df.filter(df.Mark.isNull()))
# display(df.filter(col("Mark").isNull()))
display(df.filter("Mark is Null"))

# COMMAND ----------

# Filter rows where column 'col' is not null
display(df.filter(df.Mark.isNotNull()))

# COMMAND ----------

from pyspark.sql.functions import col
# Filter rows where column 'Mark' is not null
display(df.filter(df.Mark.isNotNull()))
# Filter rows where column 'Mark' is not null using col()
display(df.filter(col("Mark").isNotNull()))
# Filter rows where column 'Mark' is not null using SQL expression
display(df.filter("Mark is Not Null"))
#Filter with and Condition
display(df.filter(df.Mark.isNotNull() & df.Attendance.isNotNull()))

# COMMAND ----------

# Using na.drop() to drop rows containing any null values
df_na_drop = df.na.drop()
# Displaying DataFrames
print("DataFrame after dropping rows with any null values:")
df_na_drop.show()

# COMMAND ----------

# Drop rows containing any null values
df_drop_any = df.na.drop(how='any')
# Display DataFrames
print("DataFrame after dropping rows with any null values:")
df_drop_any.show()

# COMMAND ----------

# Drop rows containing all null values
df_drop_all = df.na.drop(how='all')
print("\nDataFrame after dropping rows with all null values:")
df_drop_all.show()

# COMMAND ----------

display(df.na.drop(subset="Mark"))

# COMMAND ----------

display(df.na.drop(subset=["Mark","Attendance"]))

# COMMAND ----------

# Using na.fill() to fill null values in 'Mark' and 'Attendance' columns with specified values
df_na_fill = df.na.fill({'Mark': -1, 'Attendance': -1})
print("\nDataFrame after filling null values in 'Mark' and 'Attendance' columns:")
df_na_fill.show()

# COMMAND ----------

display(df.na.fill(value=0))

# COMMAND ----------

display(df.na.fill(value="NA"))

# COMMAND ----------


