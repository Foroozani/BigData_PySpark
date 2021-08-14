## read csv file
from pyspark.sql import SparkSession
from pprint import pprint
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import array_contains


spark = SparkSession.builder.appName('readdata').master('local').getOrCreate
# create a session and name it spark
pprint(spark)

##  ------------------------------------
#       read csv format
# PySpark supports reading a CSV file with a pipe, comma, tab, space, or any other delimiter/separator files.
# Using csv("path") or format("csv").load("path") of DataFrameReader, you can read a CSV file into a PySpark DataFrame.
## -------------------------------------
# file is located in a folder
data_path = "Datasets/"
students = spark.read.csv(data_path+'students.csv',header=True, inferSchema= True)
pprint(students)
students.printSchema()
"""
root
 |-- gender: string (nullable = true)
 |-- race/ethnicity: string (nullable = true)
 |-- parental level of education: string (nullable = true)
 |-- lunch: string (nullable = true)
 |-- test preparation course: string (nullable = true)
 |-- math score: integer (nullable = true)
 |-- reading score: integer (nullable = true)
 |-- writing score: integer (nullable = true)

"""
# OR

df = spark.read.format('csv').load(data_path+'students.csv')

pprint(df)
df.printSchema()
# this reads the data into DataFrame columns "_c0" for the first column and "_c1" for the second and so on
"""
root
 |-- _c0: string (nullable = true)
 |-- _c1: string (nullable = true)
 |-- _c2: string (nullable = true)
 |-- _c3: string (nullable = true)
 |-- _c4: string (nullable = true)
 |-- _c5: string (nullable = true)
 |-- _c6: string (nullable = true)
 |-- _c7: string (nullable = true)
"""
## --------------------------------------
#          read parquet format
# _______________________________________
user1 = spark.read.parquet(data_path+'user*',header=True,inferSchema=True)
user1.show(4)
user1.count()

## ---------------------------------------
#  Reading CSV files with a user-specified custom schema
# If you know the schema of the file ahead and do not want to use the inferSchema option for column names and types,
# use user-defined custom column names and type using schema option
# Refer dataset https://github.com/spark-examples/pyspark-examples/blob/master/resources/zipcodes.csv
# lets try some stuff here
df = spark.read.csv("resources/zipcodes.csv")
df.printSchema()
"""
root
 |-- _c0: string (nullable = true)
 |-- _c1: string (nullable = true)
 |-- _c2: string (nullable = true) ....
  """
# there is one option here
df2 = spark.read.option("header", True).csv("resources/zipcodes.csv")
df2.printSchema()
"""
root
 |-- RecordNumber: string (nullable = true)
 |-- Zipcode: string (nullable = true)
 |-- ZipCodeType: string (nullable = true) ...
 """
# lets add another option
df3 = spark.read.options(header = True, delimiter = ',').csv("resources/zipcodes.csv")
df3.printSchema()
df3.show(4)

# The schema does not look correct. lets change the data type

schema = StructType() \
      .add("RecordNumber",IntegerType(),True) \
      .add("Zipcode",IntegerType(),True) \
      .add("ZipCodeType",StringType(),True) \
      .add("City",StringType(),True) \
      .add("State",StringType(),True) \
      .add("LocationType",StringType(),True) \
      .add("Lat",DoubleType(),True) \
      .add("Long",DoubleType(),True) \
      .add("Xaxis",IntegerType(),True) \
      .add("Yaxis",DoubleType(),True) \
      .add("Zaxis",DoubleType(),True) \
      .add("WorldRegion",StringType(),True) \
      .add("Country",StringType(),True) \
      .add("LocationText",StringType(),True) \
      .add("Location",StringType(),True) \
      .add("Decommisioned",BooleanType(),True) \
      .add("TaxReturnsFiled",StringType(),True) \
      .add("EstimatedPopulation",IntegerType(),True) \
      .add("TotalWages",IntegerType(),True) \
      .add("Notes",StringType(),True)

df_with_schema = spark.read.option("header", True).format("csv").schema(schema).load("resources/zipcodes.csv")
df_with_schema.printSchema()
"""
root
 |-- RecordNumber: integer (nullable = true)
 |-- Zipcode: integer (nullable = true)
 |-- ZipCodeType: string (nullable = true)
 |-- City: string (nullable = true) ..."""

df3.write.option("header",True).csv("zip")  # this will create a zip folder and write the file in there with funky name
## df3.write.mode('overwrite').csv('zip.csv')
## ===========================================
# so
path = "path_to_data"
# CSV
df = spark.read.csv(path+'students.csv',inferSchema=True,header=True)

# Json
people = spark.read.json(path+'people.json')

# Parquet
parquet = spark.read.parquet(path+'users.parquet')

# Partioned Parquet
partitioned = spark.read.parquet(path+'users*')

# Parts of a partitioned Parquet
users1_2 = spark.read.option("basePath", path).parquet(path+'users1.parquet', path+'users2.parquet')