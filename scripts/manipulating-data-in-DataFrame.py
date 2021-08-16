"""
- changing data types when they are incorrectly interpretted
- Clean your data
- create new columns
- rename columns
- extract or create new value
"""

import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col, lit, to_date, trim, lower

# create a session
spark = SparkSession.builder.appName("manipulatedata").getOrCreate()
spark

##
# Trending YouTube Video Statistics, https://www.kaggle.com/datasnaek/youtube-new
path = "Datasets/"
videos = spark.read.csv(path+"youtubevideos.csv", header=True, inferSchema=True)
len(videos.columns)
videos.limit(10).show()
videos.printSchema()
videos.select("publish_time").show(5, False)
# publish_time, its schema needs to be modified. 2017-11-13T17:13:01.000Z

"""PySpark withColumn() is a transformation function of DataFrame which is used to change the value,
 convert the datatype of an existing column, create a new column, and many more

- PySpark withColumn – To change column DataType
- Transform/change value of an existing column
- Derive new column from an existing column
- Add a column with the literal value
- Rename column name
- Drop DataFrame column

here are the subclasses of the DataType in PySpark and we can change or cast DataFrame columns to *only* these types.

ArrayType, BinaryType, BooleanType, CalendarIntervalType, DateType, HiveStringType, MapType, NullType, 
NumericType, ObjectType, StringType, StructType, TimestampType

Syntax: to_date(timestamp_column,format)
"""

# create new data frame from videos DataFrame

# change type of views column
#df = videos.withColumn("views", videos["views"].cast(IntegerType()))
df = videos.withColumn("views", col("views").cast(IntegerType()))  \
     .withColumn("likes", videos.likes.cast(IntegerType()))  \
     .withColumn("dislikes",videos.dislikes.cast(IntegerType()))  \
     .withColumn("trending_date", to_date(videos.trending_date,'yy.dd.mm'))  \
     .withColumn("publish_time", to_timestamp(videos.publish_time, 'yyyy-MM-dd HH:mm:ss'))

df.describe()
df.limit(4).show()
df.printSchema()
## NOW, we face some problems here:
#1) pyspark infer trendin_date incorrectly 2017-01-14, so how to fix it???
#2) publish time is null now !!!!!!!! and it is because of funky TZ in the original format 2017-11-13T17:13:01.000Z

df = videos.withColumn("views", col("views").cast(IntegerType()))  \
     .withColumn("likes", videos.likes.cast(IntegerType()))  \
     .withColumn("dislikes",videos.dislikes.cast(IntegerType()))  \
     .withColumn("trending_date", to_date(videos.trending_date,'yy.dd.mm'))  \
#     .withColumn("publish_time", to_timestamp(videos.publish_time, 'yyyy-MM-dd HH:mm:ss'))
# create a new column
df = df.withColumn('publish_time_2', regexp_replace(df.publish_time, "T", " "))
# same for Z and replace it
df = df.withColumn("publish_time_2", regexp_replace(df.publish_time_2, 'Z', ''))
df.select('publish_time', 'publish_time_2').show(4, False)
df.printSchema()
# So now we can transform it to timestamp
df = df.withColumn('publish_time_3', to_timestamp(df.publish_time_2, 'yyyy-MM-dd HH:mm:ss.SSS'))
df.printSchema()
df.show(4)
# rename the colomn name
#renamed_df = df.withColumnRenamed("newname", "publish_time_3")  # 2017-11-13 17:13:01 the 000 just took that off, thats OK


# TRANSLATE function, alternative way
# NOte, here i am not creating object i m just showing
df.select('publish_time', translate(col('publish_time'), "TZ", " ").alias('trans_col')).show(4, False)

# -------------------------------------------------------------
# TRIM()
df = df.withColumn('title', trim(df.title))
df.select('title').show(4, False)
df = df.withColumn("title", lower(df.title))
df.select('title').show(4, False)
#--------------------------------------------------------------
# case WHEN
# option 1, when-otherwise
# option 2 expr
def CASE(args):
     pass


df.select("likes", "dislikes", expr("CASE WHEN likes > dislikes THEN 'Good mvie' "
                                    "WHEN likes < dislikes THEN 'Bad movies'  "
                                    "ELSE 'undetermined' END AS Favarability")).show(4)


df.selectExpr("likes", "dislikes", "CASE WHEN likes > dislikes THEN 'Good mvie' "
                                    "WHEN likes < dislikes THEN 'Bad movies'  "
                                    "ELSE 'undetermined' END AS Favarability").show(4)

# ---------------------------------------------------------------
# concatinate
# Joining two columns for NLP, and added
df = df.withColumn("title_channel", concat_ws(' ', df.title, df.channel_title))
df.show(4)
df.printSchema()

df.select("trending_date", year("trending_date"), month("trending_date")).show(4)

array = df.select('title', split(df.title, ' ').alias('new'))
array.show(4, False) # it return an array [we, want, to, talk, about, our, marriage],
array.select('title', array_contains(array.new, 'about')).show(4)
array.printSchema()
array.show(4, False)

# array_remove()
##
"""
Pyspark, User Defined Functions
"""
#PySpark UDF’s are similar to UDF on traditional databases. In PySpark, you create a function in a
# Python syntax and wrap it with PySpark SQL udf() or register it as udf and use it on DataFrame and SQL respectively

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

def squar(x):
    return int(x)**2

square_udf = udf(lambda z: squar(z), IntegerType())
df.select('dislikes',square_udf('dislikes')).where(df.dislikes.isNotNull())
df.select('dislikes',square_udf('dislikes')).where(df.dislikes.isNotNull()).show(4)