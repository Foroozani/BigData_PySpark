import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType,IntegerType

# PySpark applications start with initializing SparkSession which is the entry point of PySpark as below. 
#In case of running it in PySpark shell via pyspark executable, the shell automatically creates the session in the variable spark for users

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
cores = spark._jsc.sc().getExecutorMemoryStatus().keySet().size()
print("You are working with", cores, "core(s)")

print(spark)

