# -*- coding: utf-8 -*-
"""
author SparkByExamples.com
"""

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

columns = ["Seqno","Name"]
data = [("1", "john jones"),
    ("2", "tracey smith"),
    ("3", "amy sanders")]

"""
+-----+------------+
|Seqno|Names       |
+-----+------------+
|1    |john jones  |
|2    |tracey smith|
|3    |amy sanders |
+-----+------------+
"""

df = spark.createDataFrame(data=data,schema=columns)

df.show(truncate=False)
# Create a Python Function
#The first step in creating a UDF is creating a Python function. Below snippet creates a function convertCase() 
# which takes a string parameter and converts the first letter of every word to capital letter
def convertCase(str):
    resStr=""
    arr = str.split(" ")
    for x in arr:
       resStr= resStr + x[0:1].upper() + x[1:len(x)] + " "
    return resStr 

# Converting function to UDF
# Now convert this function convertCase() to UDF by passing the function to PySpark SQL udf(), this function is available at
#  org.apache.spark.sql.functions.udf package. Make sure you import this package before using it
convertUDF = udf(lambda z: convertCase(z), StringType)
# Note: The default type of the udf() is StringType hence, you can also write the above statement without return type
#  Now you can use convertUDF() on a DataFrame column as a regular build-in function.
df.select(col("Seqno"), \
    convertUDF(col("Name")).alias("Name") ) \
.show(truncate=False)
"""
+-----+-------------+
|Seqno|Name         |
+-----+-------------+
|1    |John Jones   |
|2    |Tracey Smith |
|3    |Amy Sanders  |
+-----+-------------+
"""
# Using UDF with PySpark DataFrame withColumn()
@udf(returnType=StringType()) 
def upperCase(str):
    return str.upper()

upperCaseUDF = udf(lambda z:upperCase(z),StringType())    

df.withColumn("Cureated Name", upperCase(col("Name"))) \
.show(truncate=False)

""" Using UDF on SQL """
# Registering PySpark UDF & use it on SQL
# In order to use convertCase() function on PySpark SQL, you need to register the function with PySpark by using spark.udf.register()
spark.udf.register("convertUDF", convertCase,StringType())
df.createOrReplaceTempView("NAME_TABLE")
spark.sql("select Seqno, convertUDF(Name) as Name from NAME_TABLE") \
     .show(truncate=False)
     
spark.sql("select Seqno, convertUDF(Name) as Name from NAME_TABLE " + \
          "where Name is not null and convertUDF(Name) like '%John%'") \
     .show(truncate=False)  
     
""" null check """
# UDF’s are error-prone when not designed carefully. for example, when you have a column that contains the value null on some records
columns = ["Seqno","Name"]
data = [("1", "john jones"),
    ("2", "tracey smith"),
    ("3", "amy sanders"),
    ('4',None)]

df2 = spark.createDataFrame(data=data,schema=columns)
df2.show(truncate=False)
df2.createOrReplaceTempView("NAME_TABLE2")
    
spark.udf.register("_nullsafeUDF", lambda str: convertCase(str) if not str is None else "" , StringType())

spark.sql("select _nullsafeUDF(Name) from NAME_TABLE2") \
     .show(truncate=False)

spark.sql("select Seqno, _nullsafeUDF(Name) as Name from NAME_TABLE2 " + \
          " where Name is not null and _nullsafeUDF(Name) like '%John%'") \
     .show(truncate=False)  



 

