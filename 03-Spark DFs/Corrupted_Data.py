# Databricks notebook source
df = spark.read.format('csv').option("header","true").option("inferschema","true").load("dbfs:/FileStore/tables/Production_Data_Corrupt.csv")
display(df)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
schema = StructType([
                    StructField("Month", StringType(), True),
                    StructField("Emp_count", IntegerType(), True),
                    StructField("Production_unit", IntegerType(), True),
                    StructField("Expense", IntegerType(), True),
                    StructField("_corrupt_record", StringType(), True)
])

# COMMAND ----------

#Permissive Mode

# COMMAND ----------

df1 = spark.read.format('csv').option("mode","permissive").option("header","true").schema(schema).load("dbfs:/FileStore/tables/Production_Data_Corrupt-2.csv")
display(df1)

# COMMAND ----------

#DropMalformed

# COMMAND ----------

df2 = spark.read.format('csv').option("mode","DROPMALFORMED").option("header","true").schema(schema).load("dbfs:/FileStore/tables/Production_Data_Corrupt-2.csv")
display(df2)

# COMMAND ----------

#FailFast Mode

# COMMAND ----------

df3= spark.read.format('csv').option("mode","FAILFAST").option("header","true").schema(schema).load("dbfs:/FileStore/tables/Production_Data_Corrupt-2.csv")
display(df3)

# COMMAND ----------

# FailFast Mode:

# In FailFast mode, if any malformed records or errors are encountered during the reading process, PySpark will fail immediately and raise an error, stopping further processing.
# This mode is useful when you want to ensure data integrity and prefer to halt the process when encountering any issues rather than continue with potentially corrupted data.
# Use this mode when you want strict data validation and immediate feedback on errors.
# DropMalformed Mode:

# In DropMalformed mode, if any malformed records or errors are encountered during the reading process, PySpark will simply drop those records and continue with the rest of the data.
# This mode is useful when you want to skip over problematic records and continue processing with the valid ones.
# Use this mode when you're willing to sacrifice potentially corrupted records in favor of processing the majority of valid data.
# Permissive Mode:

# In Permissive mode, PySpark will try to parse and load as much data as possible, even if there are errors or inconsistencies in the input data.
# If it encounters any malformed records or errors, PySpark will try to infer the schema and load the data accordingly, but it will also create a "_corrupt_record" column to store any problematic records.
# This mode is useful when you want to load as much data as possible, even if it means accepting some level of data corruption or inconsistency.
# Use this mode when you have semi-structured or messy data sources and need to extract as much information as possible without being overly strict about data integrity.
