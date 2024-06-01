# Databricks notebook source
sc.defaultParallelism

# COMMAND ----------

spark.conf.get("spark.sql.files.maxPartitionBytes")

# COMMAND ----------

from pyspark.sql.types import IntegerType
df = spark.createDataFrame(range(1,11), IntegerType())
display(df)

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

#Verify the data with in all partitions
df.rdd.glom().collect()

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/")

# COMMAND ----------

df1 = spark.read.format("csv").option("header",True).option("sep",",").option("inferschema",True).load("/FileStore/tables/StudentData.csv")
display(df1)

# COMMAND ----------

df1.rdd.getNumPartitions()

# COMMAND ----------

df2 = spark.read.format("csv").option("header",True).option("sep",";").option("inferschema",True).load("/FileStore/tables/stream_read/")
display(df2)

# COMMAND ----------

#5 files = 5 partitions
df2.rdd.getNumPartitions()

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/stream_read")

# COMMAND ----------

spark.conf.set("spark.sql.files.maxPartitionBytes",50)
spark.conf.get("spark.sql.files.maxPartitionBytes")

# COMMAND ----------

df3 = spark.read.format("csv").option("header",True).option("sep",";").option("inferschema",True).load("/FileStore/tables/stream_read/")
df3.rdd.getNumPartitions()

# COMMAND ----------

#repartitions

# COMMAND ----------

from pyspark.sql.types import IntegerType
df = spark.createDataFrame(range(1,21), IntegerType())
display(df)

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

#Verify the data with in all partitions
df.rdd.glom().collect()

# COMMAND ----------

df1 = df.repartition(20)
df1.rdd.getNumPartitions()
df1.rdd.glom().collect()

# COMMAND ----------

df2 = df.repartition(2)
df2.rdd.getNumPartitions()
df2.rdd.glom().collect()

# COMMAND ----------

#colease

# COMMAND ----------

from pyspark.sql.types import IntegerType
df = spark.createDataFrame(range(1,21), IntegerType())
df.rdd.getNumPartitions()
df.rdd.glom().collect()

# COMMAND ----------

df2 = df.coalesce(2)
df2.rdd.getNumPartitions()
df2.rdd.glom().collect()

# COMMAND ----------

df2 = df.coalesce(25)
df2.rdd.getNumPartitions()
df2.rdd.glom().collect()

# COMMAND ----------


