# Databricks notebook source
data_student = [
("Raja","Science",80,"P",90),
("Rakesh","Maths",90,"P",70),
("Rama","English",20,"F",80),
("Ramesh","Science",45,"F",75),
("Rajesh","Maths",30,"F",50),
("Raghav","Maths",None,"NA",70)]

# COMMAND ----------

print(data_student)

# COMMAND ----------

print(type(data_student))

# COMMAND ----------

schema = ["Name","Subject","Mark","Status","Attendence"]

# COMMAND ----------

df = spark.createDataFrame(data_student,schema=schema)
display(df)

# COMMAND ----------

from pyspark.sql.functions import when
df1 = df.withColumn("Status",when(df.Mark >=50,"Pass")
                    .when(df.Mark <50,"Fail")
                    .otherwise("Absent"))
display(df1)

# COMMAND ----------

from pyspark.sql.functions import when
df2 = df.withColumn("New_Status",when(df.Mark >=50,"Pass")
                    .when(df.Mark <50,"Fail")
                    .otherwise("Absent"))
display(df2)

# COMMAND ----------

from pyspark.sql.functions import expr
df3 = df.withColumn("new_status", expr("CASE when Mark >= 50  THEN  'Pass' " +
                        "when Mark < 50  THEN 'Fail' " +
                        "Else 'Absent' end"))
display(df3)

# COMMAND ----------

from pyspark.sql.functions import when
df4 = df.withColumn("New_Status",when(df.Mark >=50,"Pass")
                    .when(df.Mark <50,"Fail")
                    .otherwise("Absent"))
display(df4)

# COMMAND ----------

#Multi Conditions using and or or
from pyspark.sql.functions import when
df5 = df.withColumn("Grade",when((df.Mark >=80) & (df.Attendence >=80),"Distinction")
                    .when((df.Mark >= 50) & (df.Attendence >=50),"Good")
                    .otherwise("Average"))
display(df5)

# COMMAND ----------

#Multi Conditions using and or or
from pyspark.sql.functions import when
df6 = df.withColumn("Grade",when((df.Mark >=80) | (df.Attendence >=80),"Distinction")
                    .when((df.Mark >= 50) | (df.Attendence >=50),"Good")
                    .otherwise("Average"))
display(df6)

# COMMAND ----------


