# Databricks notebook source
dbutils.widgets.text("folder_name","","")
dbutils.widgets.text("file_name","","")

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

folder_location = dbutils.widgets.get("folder_name")
file_location = dbutils.widgets.get("file_name")

# COMMAND ----------

print("folder variable is",folder_location)
print("file variable is",file_location)


# COMMAND ----------

df = spark.read.format("csv").option("inferSchema",True).option("header",True).option("sep",",").load(folder_location+file_location)

# COMMAND ----------

display(df)

# COMMAND ----------


