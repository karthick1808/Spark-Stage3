# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
                    StructField("File", StringType(), True),
                    StructField("Shop", StringType(), True),
                    StructField("Sale_Count", IntegerType(), True)
])

# COMMAND ----------

dbutils.fs.mkdirs("/FileStore/tables/stream_checkpoint/")
dbutils.fs.mkdirs("/FileStore/tables/stream_read/")
dbutils.fs.mkdirs("/FileStore/tables/stream_write/")

# dbutils.fs.rm("/FileStore/tables/stream_checkpoint/",True)
# dbutils.fs.rm("/FileStore/tables/stream_read/",True)
# dbutils.fs.rm("/FileStore/tables/stream_write/",True)

# COMMAND ----------

df = spark.readStream.format("csv").schema(schema).option("header",False).option("sep",";").load("/FileStore/tables/stream_read/")
# df = df.groupBy("Shop").sum("Sale_Count")
# display(df1)

# COMMAND ----------

df2 = df.writeStream \
    .format("parquet") \
    .outputMode("appen") \
    .option("checkpointLocation", "/FileStore/tables/stream_checkpoint/") \
    .option("path", "/FileStore/tables/stream_write/") \
    .start().awaitTermination()

# COMMAND ----------

df3 = spark.readStream.format("parquet").schema(schema).load("/FileStore/tables/stream_write/*.parquet")
display(df3)

# COMMAND ----------


