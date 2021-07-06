##
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("session").master("local").getOrCreate()

path = "Datasets/"
data = spark.read.csv(path+"pga_tour_historical.csv",inferSchema=True, header=True)
data.show(10)
data.limit(10).toPandas()
data.count()
data.printSchema()
data.describe
# Generate summary statistics for TWO variables

data.select('Season', 'Value').summary("count","min",'max').show()

# Write a partioned parquet file
## Now try writing a parquet file (not partitioned) from the pga dataset. But first create a new dataframe containing
# ONLY the the "Season" and "Value" fields (using the "select command you used in the question above) and write a parquet file
# partitioned by "Season". This is a bit of a challenge aimed at getting you ready for material that will be covered later on
# in the course. Don't feel bad if you can't figure it out.
df = data.select('Season','Value')
# it will create a directory named partition_parquet
df.write.mode("overwrite").parquet("partition_parquet/", partitionBy='Season')
# then partition parquet-data in that directory
#df.write.mode("overwrite").partitionBy("Season").parquet("partition_parquet/")
df.show(20)

# Now try reading in the partitioned parquet file you just created above.
path_prq = 'partition_parquet/'
parquet = spark.read.parquet(path_prq)
parquet.show(20)
df.printSchema()

# Reading in a set of paritioned parquet files
# # Now try only reading Seasons 2010, 2011 and 2012.

partitioned = spark.read.parquet(path_prq+'Season=2010/',path_prq+'Season=2011/',
                                 path_prq+'Season=2012/')

partitioned.show(10)
# we need to use a method to get both season and value

partitioned = spark.read.option("basePath", path_prq).parquet(path_prq+'Season=2010/',path_prq+'Season=2011/',
                                 path_prq+'Season=2012/')

partitioned.show(10)
