# Databricks notebook source
print(spark.sparkContext.version)
#there is no difference beween union and union all
# in version 2.0.0 same as union but retains duplicates
#in newer version dropDuplicates we need to use

# COMMAND ----------

employee_data = [
(100,"Stephen","1999","100","M",2000),
(200,"Philip","2002","200","M",8000),
(300,"John","2010","100","",6000)
]

employee_schema = ["employee_id","name","doj","employee_dept_id","gender","salary"]

# COMMAND ----------

print(employee_data)

# COMMAND ----------

print(employee_schema)

# COMMAND ----------

df1 = spark.createDataFrame(employee_data,schema=employee_schema)
display(df1)

# COMMAND ----------

employee_data1 = [
(600,"john","2010","100","M",6000),
(700,"nancy","2008","400","F",10000),
(300,"John","2010","100","",6000),
(400,"Rosy","2014","500","M",5000)
]

employee_schema1 = ["employee_id","name","doj","employee_dept_id","gender","salary"]

# COMMAND ----------

df2 = spark.createDataFrame(employee_data1,schema=employee_schema1)
display(df2)

# COMMAND ----------

df3 = df1.union(df2)
display(df3)

# COMMAND ----------

df4 = df3.dropDuplicates()
display(df4)

# COMMAND ----------

df4 = df1.unionAll(df2)
display(df4)

# COMMAND ----------

employee_data2 = [
(600,"john","2010","100","M"),
(700,"nancy","2008","400","F"),
(300,"John","2010","100",""),
(400,"Rosy","2014","500","M")
]

employee_schema2 = ["employee_id","name","doj","employee_dept_id","gender"]

# COMMAND ----------

df5 = spark.createDataFrame(employee_data2,schema=employee_schema2)
display(df5)

# COMMAND ----------

df6 = df2.union(df5)

# COMMAND ----------


