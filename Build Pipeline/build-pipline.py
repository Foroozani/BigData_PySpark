import findspark
#findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *      # convert datatype from one type to another
from pyspark.sql.functions import *  # manipulation of data

spark = SparkSession.builder.getOrCreate()
df = spark.sql("select 'name' as colomn")   # create a dataframe
df.show()

# Configure spark session with 2 cores for this job
spark = SparkSession\
    .builder\
    .master('local[2]')\
    .appName('quake_etl')\
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:2.4.1')\
    .getOrCreate()

spark
# Load the dataset from https://github.com/EBISYS/WaterWatch
df_load = spark.read.csv(r"Datasets/database.csv", header=True)
# Preview df_load
df_load.take(1)
df_load.columns
df_load.printSchema()

# Drop fields we don't need from df_load
lst_dropped_columns = ['Depth Error', 'Time', 'Depth Seismic Stations',
                       'Magnitude Error','Magnitude Seismic Stations','Azimuthal Gap',
                       'Horizontal Distance','Horizontal Error',
                       'Root Mean Square','Source','Location Source','Magnitude Source','Status']

df_load = df_load.drop(*lst_dropped_columns)
# Preview df_load
df_load.show(5)
# Create a "year" field and add it to the dataframe
df_load = df_load.withColumn('Year', year(to_timestamp('Date', 'dd/MM/yyyy')))
# Preview df_load
df_load.show(5)
# Build the quakes frequency dataframe using the year field and counts for each year
df_quake_freq = df_load.groupBy('Year').count().withColumnRenamed('count', 'Counts')
# Preview df_quake_freq
df_quake_freq.show(5)

# Preview df_load schema
df_load.printSchema()

# Cast some fields from string into numeric types
df_load = df_load.withColumn('Latitude', df_load['Latitude'].cast(DoubleType()))\
    .withColumn('Longitude', df_load['Longitude'].cast(DoubleType()))\
    .withColumn('Depth', df_load['Depth'].cast(DoubleType()))\
    .withColumn('Magnitude', df_load['Magnitude'].cast(DoubleType()))

# Preview df_load
df_load.show(5)

# Preview df_load schema
df_load.printSchema()

# Create avg magnitude and max magnitude fields and add to df_quake_freq
df_max = df_load.groupBy('Year').max('Magnitude').withColumnRenamed('max(Magnitude)', 'max_magnitude')
df_avg = df_load.groupBy('Year').avg('Magnitude').withColumnRenamed('avg(Magnitude)', 'avg_magnitude')

df_avg.show(5)

# Join df_max, and df_avg to df_quake_freq
df_quake_freq = df_quake_freq.join(df_avg, ['Year']).join(df_max, ['Year'])
# Preview df_quake_freq
df_quake_freq.printSchema()

# Remove nulls
df_load.dropna()
df_quake_freq.dropna()