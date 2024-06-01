# Databricks notebook source
Transaction = [
    (100, 'Cosmetic', 150),
    (200, 'Apparel', 250), 
    (300, 'Shirt', 400),
    (400, 'Trouser', 500),
    (500, 'Socks', 20),
    (100, 'Belt', 70),
    (200, 'Cosmetic', 250),
    (300, 'Shoe', 400),
    (400, 'Socks', 25),
    (500, 'Shorts', 100)
]


# COMMAND ----------

#Fact Table
transactionDF = spark.createDataFrame(data=Transaction, schema = ["StoreID","Item","Amount"])
display(transactionDF)

# COMMAND ----------

Store = [
    (100, 'Store_London'),
    (200, 'Store_Paris'),
    (300, 'Store_Frankfurt'),
    (400, 'Store_Stockholm'),
    (500, 'Store_Oslo')
]


# COMMAND ----------

#Dimesnion Table
storeDF = spark.createDataFrame(data=Store, schema = ["StoreID","StoreName"])
display(storeDF)

# COMMAND ----------

from pyspark.sql.functions import broadcast
joinDF = transactionDF.join(broadcast(storeDF),transactionDF["StoreID"] == storeDF["StoreID"] )
display(joinDF)

# COMMAND ----------

joinDF.explain()

# COMMAND ----------

joinDF.explain(True)

# COMMAND ----------


