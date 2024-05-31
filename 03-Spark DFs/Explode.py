# Databricks notebook source
#create Dataframe with array column
array_appliance = [
('Raja',['TV','Refrigerator','Oven','AC']),
('Raghav',['AC','Washing Machine',None]),
('Ram',['Grinder','TV']),
('Ramesh',['Refrigerator','TV',None]),
('Rajesh',None),
]

# COMMAND ----------

print(array_appliance)

# COMMAND ----------

print(type(array_appliance))

# COMMAND ----------

df_app = spark.createDataFrame(array_appliance,schema=["Name","Appliances"])

# COMMAND ----------

display(df_app)

# COMMAND ----------

df_app.printSchema()

# COMMAND ----------


#create Dataframe with map column
map_brand = [
('Raja',{'TV':'LG','Refrigerator':'Samsung','Oven':'Philips','AC':'Voltas'}),
('Raghav',{'AC':'Samsung','Washing Machine':'LG'}),
('Ram',{'Grinder':'Preethi','TV':''}),
('Ramesh',{'Refrigerator':'LG','TV':'Crome'}),
('Rajesh',None),
]

# COMMAND ----------

print(map_brand)

# COMMAND ----------

print(type(map_brand))

# COMMAND ----------

df_brand = spark.createDataFrame(map_brand,schema=["Name","Brand"])

# COMMAND ----------

display(df_brand)

# COMMAND ----------

df_brand.printSchema()

# COMMAND ----------

df_app.printSchema()

# COMMAND ----------

#Explode array
from pyspark.sql.functions import explode
df2 = df_app.select(df_app.Name,explode(df_app.Appliances))

# COMMAND ----------

display(df2)

# COMMAND ----------

df_brand.printSchema()

# COMMAND ----------

#explode map field
from pyspark.sql.functions import explode
df3 = df_brand.select(df_brand.Name,explode(df_brand.Brand))

# COMMAND ----------

display(df3)

# COMMAND ----------

#Explode array outer = consider null value
from pyspark.sql.functions import explode_outer
df4 = df_app.select(df_app.Name,explode_outer(df_app.Appliances))
display(df4)

# COMMAND ----------

#explode map field outer = consider null value
from pyspark.sql.functions import explode_outer
df3 = df_brand.select(df_brand.Name,explode_outer(df_brand.Brand))
display(df3)

# COMMAND ----------

#Positional Explode
from pyspark.sql.functions import posexplode
df6 = df_app.select(df_app.Name,posexplode(df_app.Appliances))
display(df6)
df7 = df_brand.select(df_brand.Name,posexplode(df_brand.Brand))
display(df7)

# COMMAND ----------

#Positional Explode with outer consider null value
from pyspark.sql.functions import posexplode_outer
df8 = df_app.select(df_app.Name,posexplode_outer(df_app.Appliances))
display(df8)
df9 = df_brand.select(df_brand.Name,posexplode_outer(df_brand.Brand))
display(df9)

# COMMAND ----------


