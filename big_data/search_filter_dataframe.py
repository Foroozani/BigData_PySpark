import pyspark
from pyspark.sql import SparkSession
# May take awhile locally
spark = SparkSession.builder.appName("FunctionsHW").getOrCreate()

cores = spark._jsc.sc().getExecutorMemoryStatus().keySet().size()
print("You are working with", cores, "core(s)")
spark

fifa = spark.read.csv('Datasets/fifa19.csv',inferSchema=True,header=True)

print(fifa.printSchema())

from pyspark.sql.functions import *
fifa.select(['Name','Position','Release Clause']).show(5,False)
# Display the same results from above sorted by the players names
fifa.select(['Name','Position']).orderBy('Name').show(5)
fifa.select(['Name','Position','Age']).orderBy(fifa['Age'].desc()).show(5)

# Select only the players who belong to a club begining with FC
# One way
fifa.select("Name","Club").where(fifa.Club.like("FC%")).show(5, False)

# Another way
fifa.select("Name","Club").where(fifa.Club.startswith("FC")).limit(4).toPandas()

## ======================================================
# to create a new dataframe
df = fifa.limit(100)
df.count()

# if we slice the colomns
df2_col = fifa.columns[0:5]
df2 = fifa.select(df2_col)
df2.count()
df2.show(5,False)
# count the colomn 
len(df2.columns)
# ========================================================
# Filtering data with condition
# ========================================================

fifa.filter("Age>40").select(['Name','Age']).limit(4).toPandas()
