#!/usr/bin/env python
# coding: utf-8

# # Reading Writing and Validating Data in PySpark
# 
# Welcome to PySpark!
# 
# In this first lecture, we will be covering:
# 
#  - Reading in Data
#  - Partioned Files
#  - Validating Data
#  - Specifying Data Types
#  - Writing Data
# 
# Below you will see the script to begin your first PySpark instance. If you're ever curious
# about how your PySpark instance is performing, Spark offers a neat Web UI with tons of information.
# Just navigate to http://[driver]:4040 in your browswer where "drive" is you driver name.
# If you are running PySpark locally, it would be http://localhost:4040 or you can use the hyperlink
# automatically produced from the script below.

# First let's create our PySpark instance!

# PC users can use the next two lines of code but mac users don't need it
# import findspark
# findspark.init()

import pyspark       # only run after findspark.init()
from pyspark.sql import SparkSession
# May take awhile locally
spark = SparkSession.builder.appName("ReadWriteValidate").getOrCreate()
spark
cores = spark._jsc.sc().getExecutorMemoryStatus().keySet().size()
print("You are working with", cores, "core(s)")

# ## Reading data
# 
# A DataFrame is equivalent to a relational table in Spark SQL, and can be created using various
# functions in SparkSession.
# 
# First let's try reading in a csv file containing a list of students and their grades.
# **Source:** https://www.kaggle.com/spscientist/students-performance-in-exams

# Start by reading a basic csv dataset
# Let Spark know about the header and infer the Schema types!

path ="Datasets/"
# Some csv data
students = spark.read.csv(path+'students.csv',inferSchema=True,header=True)
students.limit(4).toPandas()

# **Parquet Files**
# Now try reading in a parquet file. This is most common data type in the big data world.
# Why? because it is the most compact file storage method (even better than zipped files!)

parquet = spark.read.parquet(path+'users1.parquet')
parquet.show(2)
parquet.count()

# **Partitioned Parquet Files**
# 
# Actually most big datasets will be partitioned. Here is how you can collect all
# the pieces (parts) of the dataset in one simple command.

partitioned = spark.read.parquet(path+'users*')
partitioned.show(2)

# You can also opt to read in only a specific set of paritioned parquet files.
# Say for example that you only wanted users1 and users2 and not users3

# Note that the .option("basePath", path) option is used to override the automatic function
# that will exclude the partitioned variable in resulting dataframe. 
# I prefer to have the partitioning info in my new dataframe personally. 
users1_2 = spark.read.option("basePath", path).parquet(path+'users1.parquet',
                                                       path+'users2.parquet')
users1_2.show(4)
users1_2.count()
#-----------------------------------------------------------
#in **AWS** cloud storing data in s3 buckets your code will be more like this...
bucket = "my_bucket"
key1 = "partition_test/Table1/CREATED_YEAR=2015/*"
key2 = "partition_test/Table1/CREATED_YEAR=2017/*"
key3 = "partition_test/Table1/CREATED_YEAR=2018/*"

test_df = spark.read.parquet('s3://'+bucket+'/'+key1,\
                             's3://'+bucket+'/'+key2,\
                             's3://'+bucket+'/'+key3)

test_df.show(1)
#---------------------------------------------------------

# ## Validating Data
# 
# Next you will want to validate that you dataframe was read in correct. We will get
# into more detailed data evaluation later on but first we need to ensure that all the
# variable types were infered correctly and that the values actually made it in... sometimes
# they don't :)
students.printSchema()      #Prints out the schema in the tree format.
students.columns
students.describe
# Get an inital view of your dataframe
students.show(3)

# Note the types here:
print(type(students))
studentsPdf = students.toPandas()
print(type(studentsPdf))


# A Solid Summary of your data:
#show the data (like df.head())
print(students.printSchema())
print("")
print(students.columns)
print("")
print(students.describe()) # Not so fond of this one but to each their own

# If you need to get the type of just ONE column by name you can use this function:
students.schema['math score'].dataType

# Neat "describe" function
students.describe(['math score']).show()


# Summary function
students.select("math score", "reading score","writing score").summary("count", "min", "25%", "75%", "max").show()

#  How to specify data types as you read in datasets.
# Some data types make it easier to infer schema (like tabular formats such as csv which we will show later). 
# 
# However you often have to set the schema yourself if you aren't dealing with a .read method that
# doesn't have inferSchema() built-in.
# Spark has all the tools you need for this, it just requires a very specific structure:

from pyspark.sql.types import StructField,StringType,IntegerType,StructType,DateType

# Next we need to create the list of Structure fields
#     * :param name: string, name of the field.
#     * :param dataType: :class:`DataType` of the field.
#     * :param nullable: boolean, whether the field can be null (None) or not.

data_schema = [StructField("name", StringType(), True),
               StructField("email", StringType(), True),
               StructField("city", StringType(), True),
               StructField("mac", StringType(), True),
               StructField("timestamp", DateType(), True),
               StructField("creditcard", StringType(), True)]

final_struc = StructType(fields=data_schema)


# a .json file  
# 
# **Source:** https://gist.github.com/raine/da15845f332a2fb8937b344504abfbe0

people = spark.read.json(path+'people.json', schema=final_struc)

people.printSchema()


# ## Writing Data
# First let's just try writing a simple csv file.

# Note the funky naming convention of the file in your output folder. There is no way to directly change this. 
students.write.mode("overwrite").csv('write_test.csv')

# students.write.csv('write_test.csv')
students.toPandas().to_csv('write_test2.csv')
# Note the strange naming convention of the output file in the path that you specified.
# Spark uses Hadoop File Format, which requires data to be partitioned - that's why you have part- files.
# If you want to rename your written files to a more user friendly format, you can do that using the
# method below:
from py4j.java_gateway import java_import
java_import(spark._jvm, 'org.apache.hadoop.fs.Path')

fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
file = fs.globStatus(spark._jvm.Path('write_test.csv/part*'))[0].getPath().getName()
fs.rename(spark._jvm.Path('write_test.csv/' + file), spark._jvm.Path('write_test2.csv'))
#these two need to be different
fs.delete(spark._jvm.Path('write_test.csv'), True)


#  Writting Parquet files
# 
# Now let's try writing a parquet file. This is best practice for big data as it is the most compact
# storage method.

users1_2.write.mode("overwrite").parquet('parquet/')

# For those who got an error attempting to run the above code.
# Try this solution: https://stackoverflow.com/questions/59220832/unable-to-write-spark-dataframe-to-a-parquet-file-format-to-c-drive-in-pyspark
# 
#  Writting Partitioned Parquet Files
# 
# Now try to write a partioned parquet file... super fun!

users1_2.write.mode("overwrite").partitionBy("gender").parquet('part_parquet/')


# #### Writting your own dataframes here!
# 
# You can also create your own dataframes directly here in your Juypter Notebook too if you want. 


values = [('Pear',10),('Orange',36),('Banana',123),('Kiwi',48),('Peach',16),('Strawberry',1)]
df = spark.createDataFrame(values,['fruit','quantity'])
df.show()

