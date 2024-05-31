# Databricks notebook source
dbutils.help()

# COMMAND ----------

dbutils.fs.help()


# COMMAND ----------

dbutils.fs.help('cp')

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

dbutils.credentials.help()

# COMMAND ----------

#filesystem
dbutils.fs.ls("/FileStore")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables")

# COMMAND ----------

#content of the file
dbutils.fs.head("/FileStore/tables/StudentData.csv")

# COMMAND ----------

#New Folder
dbutils.fs.mkdirs("/FileStore/tables/data/")

# COMMAND ----------

dbutils.fs.mkdirs("/FileStore/data/")

# COMMAND ----------

dbutils.fs.mkdirs("/Data/")

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables')

# COMMAND ----------

dbutils.fs.cp("/FileStore/tables/StudentData.csv",'/Data')

# COMMAND ----------

dbutils.fs.ls("/Data/")

# COMMAND ----------

dbutils.fs.mv("/FileStore/tables/WordData-1.txt","/Data")

# COMMAND ----------

dbutils.fs.ls("/Data/")

# COMMAND ----------

dbutils.fs.put("/Data/demo.txt","Hello world")

# COMMAND ----------

dbutils.fs.ls("/Data/")

# COMMAND ----------

dbutils.fs.head("/Data/demo.txt")

# COMMAND ----------

dbutils.fs.rm("/Data/demo.txt")

# COMMAND ----------

dbutils.fs.ls("/Data")

# COMMAND ----------

dbutils.fs.rm("/Data",True)

# COMMAND ----------

dbutils.fs.ls("/FileStore")

# COMMAND ----------

dbutils.fs.rm("/FileStore/data/",True)

# COMMAND ----------

dbutils.fs.ls("/FileStore")

# COMMAND ----------

#Notebook
dbutils.notebook.run("/21march-3",60)

# COMMAND ----------

# MAGIC %run ./widgets $folder_name = "/FileStore/tables" $file_name = "/StudentData.csv"

# COMMAND ----------

# Create the list of choices (numbers from 1 to 9 as strings)
choices = [str(x) for x in range(1, 11)]
# Create the dropdown widget
dbutils.widgets.dropdown("drop_down", "1", choices)

# COMMAND ----------

# Create the list of choices (numbers from 1 to 9 as strings)
choice = [str(x) for x in range(1, 11)]
# Create the dropdown widget
dbutils.widgets.combobox("combo_box", "1", choice)

# COMMAND ----------

dbutils.widgets.multiselect("product", "Camera", ("Camera", "GPS", "SmartPhone"))

# COMMAND ----------

dbutils.widgets.remove("combo_box")

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------


