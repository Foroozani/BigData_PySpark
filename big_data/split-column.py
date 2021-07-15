# PySpark split() Column into Multiple Columns
from sqlite3 import Timestamp

import pyspark
from pyspark.sql import SparkSession
# from pyspark.sql.functions import split
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
spark=SparkSession.builder.appName("sparkbyexamples").getOrCreate()
##
#
# create a data which is a list
data = [('James','','Smith','1991-04-01'),
  ('Michael','Rose','','2000-05-19'),
  ('Robert','','Williams','1978-09-05'),
  ('Maria','Anne','Jones','1967-12-01'),
  ('Jen','Mary','Brown','1980-02-17')
]

df0 = spark.createDataFrame(data, ["name", "midname", "surname", "dob"])

df0.printSchema()
df0.show(truncate=False)
"""
+-------+-------+--------+----------+
|name   |midname|surname |dob       |
+-------+-------+--------+----------+
|James  |       |Smith   |1991-04-01|
|Michael|Rose   |        |2000-05-19|
|Robert |       |Williams|1978-09-05|
|Maria  |Anne   |Jones   |1967-12-01|
|Jen    |Mary   |Brown   |1980-02-17|
+-------+-------+--------+----------+
"""
#----------------------------
# Below example creates a new Dataframe with Columns year, month, and the day after performing a split()
# function on dob Column of string type.

df1 = df0.withColumn('year', split(df0['dob'], '-').getItem(0)) \
       .withColumn('month', split(df0['dob'], '-').getItem(1)) \
       .withColumn('day', split(df0['dob'], '-').getItem(2))
df1.show(truncate=False)
#---------
split_col = pyspark.sql.functions.split(df0['dob'], '-')
df2 = df0.withColumn('year', split_col.getItem(0)) \
       .withColumn('month', split_col.getItem(1)) \
       .withColumn('day', split_col.getItem(2))
df2.show(truncate=False)

"""
+-------+-------+--------+----------+----+-----+---+
|name   |midname|surname |dob       |year|month|day|
+-------+-------+--------+----------+----+-----+---+
|James  |       |Smith   |1991-04-01|1991|04   |01 |
|Michael|Rose   |        |2000-05-19|2000|05   |19 |
|Robert |       |Williams|1978-09-05|1978|09   |05 |
|Maria  |Anne   |Jones   |1967-12-01|1967|12   |01 |
|Jen    |Mary   |Brown   |1980-02-17|1980|02   |17 |
+-------+-------+--------+----------+----+-----+---+
"""

# Using split() function of Column class
split_col = pyspark.sql.functions.split(df0['dob'], '-')
df3 = df0.select("firstname","middlename","lastname","dob", split_col.getItem(0).alias('year'),split_col.getItem(1).alias('month'),split_col.getItem(2).alias('day'))
df3.show(truncate=False)









