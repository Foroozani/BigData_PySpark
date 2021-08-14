##
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pprint import pprint
# May take awhile locally
spark = SparkSession.builder.appName("FunctionsHW").getOrCreate()

cores = spark._jsc.sc().getExecutorMemoryStatus().keySet().size()
print("You are working with", cores, "core(s)")
spark
##
df = spark.read.csv('Datasets/df19.csv',inferSchema=True,header=True)
print(df.printSchema())
df.limit(4).toPandas()
df.show(4, truncate = True)
len(df.columns) # 89
df.describe

df.select(['Aggression', 'Stamina']).show(5)
df.select(['Aggression', 'Stamina']).summary('count','min').show()
df.select(['Name', 'Age']).orderBy(df['Age'].desc()).show(5)
df.select(['*']).show(5)

## filtering data horizontally WHERE condition
df.select(["Name", "age", "Club"]).show(5)
df.select(["Name", "age", "Club"]).where(df.Club.like("%celon%")).show(5)

# ------------------------------
# SELECT SUBSTRING ('what a wonderful DAY' from 2 for 6); -- hat a
df.select("Photo",df.Photo.substr(-4,5).alias('the last 4 charachter')).show(5)
# ISIN(list)
df['Name','club','Nationality'].filter("Club IN ('FC Barcelona')").limit(4).toPandas()

df.select('Name').where(df.Name.startswith("L")).show(5)
df.select('Name', 'Club').where(df.Name.startswith("L")).where(df.Name.endswith('i')).where(df.Club.like('%Barcelona')).show(5)

## SLICING DataFrame, take n number of rows
df.count()
df1 = df.limit(100)
df1.show(5, True).toPandas()

# SLICING, take n number of colomns
df2 = df.select('Name', 'Club', "Nationality")
# OR
df_sel_col = df.select(df.columns[0:5])
#
df2.limit(5).show()
len(df2.columns)   #3
df2.count()        #18207
len(df_sel_col.columns)   #5
df_sel_col.limit(5).show()

df.printSchema()
df['Name', 'Weight'].filter("Overall>50").limit(4).show()
df['Name', 'Weight'].limit(4).show()
df.select(['Name','Position','Release Clause']).show(5,False)
# Display the same results from above sorted by the players names
df.select(['Name','Position']).orderBy('Name').show(5)
df.select(['Name','Position','Age']).orderBy(df['Age'].desc()).show(5)

# Select only the players who belong to a club begining with FC
# One way
df.select("Name","Club").where(df.Club.like("FC%")).show(5, False)

# Another way
df.select("Name","Club").where(df.Club.startswith("FC")).limit(4).toPandas()

## ======================================================
# to create a new dataframe
df = df.limit(100)
df.count()

# if we slice the colomns
df2_col = df.columns[0:5]
df2 = df.select(df2_col)
df2.count()
df2.show(5,False)
# count the colomn 
len(df2.columns)

df.filter("Age>40").select(['Name','Age']).limit(4).toPandas()
#   COLLECTING RESULTS AS OBJECTS ----> .COLLECT() method, it will collect results as a python object
df4 = df.select('Name', 'Club').where(df.Name.startswith("L")).collect()    #object is list
df4.toPandas()
type(df4[0])   # 'pyspark.sql.types.Row'
print("Name start with L:",df4[1][1])