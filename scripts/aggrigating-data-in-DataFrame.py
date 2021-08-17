"""
- Group by
- Pivot
- Aggregate method
- Combos of each
"""
#from pyspark.sql.functions import mean

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('aggrigation').getOrCreate()
spark
df0 = spark.read.csv("Datasets/nyc_air_bnb.csv", header=True, inferSchema=True)
df0.printSchema()
df0.show(4)

from pyspark.sql.types import *
from pyspark.sql.functions import *

df = df0.withColumn("price", df0.price.cast(IntegerType()))
df.printSchema()

df = df.withColumn("minimum_nights", df.minimum_nights.cast(IntegerType())) \
    .withColumn("number_of_reviews", df.number_of_reviews.cast(IntegerType())) \
    .withColumn("reviews_per_month", df.reviews_per_month.cast(IntegerType())) \
    .withColumn("calculated_host_listings_count", df.calculated_host_listings_count.cast(IntegerType())) \
    .withColumn("last_review", df.last_review.cast('date'))

df.printSchema()
df.show(4, False)

# GROUP BY
df.groupBy("neighbourhood_group").min().show(5)
df.summary().show(5)
df.summary("min","max","count").show(5)
df.select('price', 'minimum_nights').summary('min','max','count', 'mean').show(5)
##
df.select(countDistinct("neighbourhood_group"), mean('price'), max('price')).show(5)

df.groupBy('room_type').pivot("neighbourhood_group", ["Queens", "Brooklyn"]). count().show(5)
