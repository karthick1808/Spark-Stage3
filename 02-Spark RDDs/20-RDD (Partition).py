# The parameter sc.defaultParallelism determines the number of partitions when creating data within Spark.
# Dafault value is 8 so it creates 8 partition by default.
# • When reading data from external system, partitions are created based on parameter
# spark.sql.files.maxPartitionBytes which is by default 128
# MB

sc.defaultParallelism

spark.conf.get("spark.sql.files.maxPartitionBytes")
# Adopting best partition strategy is designing best performance in spark application.
# The right number of partitions created based on number of cores boosts the performance. If not, hits the performance
# Evenly distributed partition improves the performance, unevenly distributed performance hits the performance
# Lets say only one partition is created with size of 500 MB in a worker node with 16 cores. One partition can't be shared among cores. So one core would be processing 500 MB data where 15 cores are kept idle.
# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Partition")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/sample_words2.txt')
rdd = rdd.repartition(5)
rdd2 = rdd.flatMap(lambda x: x.split(' '))
rdd3 = rdd2.map(lambda x: (x,1))


# COMMAND ----------

rdd3.saveAsTextFile('/FileStore/tables/output/5partitionOutput')


# Repartition
# Function repartition is used to increase or decrease partitions in Spark
# Repartition always shuffles the data and build new partitions from scratch
# Repartition results in almost equal sized partitions.
# Due to full shuffle, it is not good for performance in some use cases. But as it creates equal sized partitions, good for performance in some use cases.

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/sample_words2.txt')
rdd = rdd.repartition(5)
rdd2 = rdd.flatMap(lambda x: x.split(' '))
rdd3 = rdd2.map(lambda x: (x,1))
rdd3 = rdd3.coalesce(3)

# COMMAND ----------

rdd3.saveAsTextFile('/FileStore/tables/output/3partitionOutput')

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/output/5partitionOutput')
rdd.collect()
