# PySpark provides two main options when it comes to using staight SQL. Spark SQL and SQL Transformer.
# ## 1. Spark SQL
# Spark TempView provides two functions that allow users to run **SQL** queries against a Spark DataFrame:
#
#  - **createOrReplaceTempView:** The lifetime of this temporary view is tied to the SparkSession that was
#  used to create the dataset. It creates (or replaces if that view name already exists) a lazily evaluated
#  "view" that you can then use like a hive table in Spark SQL. It does not persist to memory unless you cache
#  the dataset that underpins the view.
#  - **createGlobalTempView:** The lifetime of this temporary view is tied to this Spark application.
#  This feature is useful when you want to share data among different sessions and keep alive until your
#  application ends.
#

import pyspark # only run after findspark.init()
from pyspark.sql import SparkSession
# May take awhile locally
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

cores = spark._jsc.sc().getExecutorMemoryStatus().keySet().size()
print("You are working with", cores, "core(s)")
spark

##**Source:** https://www.kaggle.com/r3w0p4/recorded-crime-data-at-police-force-area-level
# Start by reading a basic csv dataset
path = 'Datasets/'
crime = spark.read.csv(path+"rec-crime-pfa.csv",header=True,inferSchema=True)

# So, in order for us to perform SQL calls off of this dataframe, we will need to rename any variables
# that have spaces in them. We will not be using the first variable so we'll leave that one as is,
# but we will be using the last variable, so I will go ahead and change that to Count so we can work with it.

df = crime.withColumnRenamed('Rolling year total number of offences','Count')
#.withColumn("12 months ending", crime["12 months ending"].cast(DateType())).
print(df.printSchema())


# Create a temporary view of the dataframe, it is like a hive table in Spark SQL
df.createOrReplaceTempView("newtable")
spark.sql("SELECT * FROM newtable WHERE Count > 1000").limit(5).toPandas()
spark.sql("SELECT sum(Count) as total FROM newtable where Count between 1000 and 2000").show(5)
spark.sql("SELECT Region, sum(Count) as total FROM newtable GROUP BY Region").show(5)

