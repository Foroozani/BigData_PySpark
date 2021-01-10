import pyspark 
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Create the spark session
spark = SparkSession\
    .builder\
    .master('local[2]')\
    .appName('quakes_etl')\
    .config('spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:2.4.1')\
    .getOrCreate()

# Load the dataset
df_load = spark.read.csv(r"C:\Users\Edwin\Downloads\database.csv", header=True)

# Remove all fields we don't need
lst_dropped_columns = ['Depth Error', 'Time', 'Depth Seismic Stations','Magnitude Error','Magnitude Seismic Stations','Azimuthal Gap', 'Horizontal Distance','Horizontal Error',
    'Root Mean Square','Source','Location Source','Magnitude Source','Status']

df_load = df_load.drop(*lst_dropped_columns)

# Create a year field and add it to the df_load dataframe
df_load = df_load.withColumn('Year', year(to_timestamp('Date', 'dd/MM/yyyy')))

# Create the quakes freq dataframe form the year and count values
df_quake_freq = df_load.groupBy('Year').count().withColumnRenamed('count', 'Counts')

# Cast string fields to double types
df_load = df_load.withColumn('Latitude', df_load['Latitude'].cast(DoubleType()))\
    .withColumn('Longitude', df_load['Longitude'].cast(DoubleType()))\
    .withColumn('Depth', df_load['Depth'].cast(DoubleType()))\
    .withColumn('Magnitude', df_load['Magnitude'].cast(DoubleType()))

# Create avg and max magnitude fields and add to df_quake_freq
df_max = df_load.groupBy('Year').max('Magnitude').withColumnRenamed('max(Magnitude)', 'Max_Magnitude')
df_avg = df_load.groupBy('Year').avg('Magnitude').withColumnRenamed('avg(Magnitude)', 'Avg_Magnitude')

# Join the max and avg dfs to df_quake_freq
df_quake_freq = df_quake_freq.join(df_avg, ['Year']).join(df_max, ['Year'])

# Remove records with null values
df_load.dropna()
df_quake_freq.dropna()

# Load df_load into mongodb
df_load.write.format('mongo')\
    .mode('overwrite')\
    .option('spark.mongodb.output.uri', 'mongodb://127.0.0.1:27017/Quake.quakes').save()

# Load df_quake_freq into mongodb
df_quake_freq.write.format('mongo')\
    .mode('overwrite')\
    .option('spark.mongodb.output.uri', 'mongodb://127.0.0.1:27017/Quake.quake_freq').save()

# Print dataframe heads
print(df_quake_freq.show(5))
print(df_load.show(5))

print('INFO: Job ran successfully')
print('')


# submit job: spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:2.4.1 data_ETL.py

















