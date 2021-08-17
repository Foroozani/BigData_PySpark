"""
- Appending Table
- Joining Tables
"""

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('jointables').getOrCreate()
spark
##
valuesP = [('koala',1,'yes'),('caterpillar',2,'yes'),('deer',3,'yes'),('human',4,'yes')]
plants = spark.createDataFrame(valuesP,['name','id','eats_plants'])

valuesM = [('shark',5,'yes'),('lion',6,'yes'),('tiger',7,'yes'),('human',4,'yes')]
meat = spark.createDataFrame(valuesM,['name','id','eats_meat'])
##
print("Plant eaters (herbivores)")
print(plants.show())
print("Meat eaters (carnivores)")
print(meat.show())

# ---------------

innerjoinDF = plants.join(meat, on = ['name', 'id'], how='inner')
innerjoinDF.show()

leftjoinDF = plants.join(meat, on = 'name', how='left')
leftjoinDF.show()

rightjoinDF = plants.join(meat, on = 'name', how='right')
rightjoinDF.show()

# to exclude a value from a join table
rightjoinDF = plants.join(meat, on = 'name', how='right').filter(plants.name.isNotNull())
rightjoinDF.show()

# FULL outer join
fulljoinDF = plants.join(meat, on = 'name', how='full')
fulljoinDF.show()

##
import os
"""
#  - **course_offerings:** uuid, course_uuid, term_code, name
#  - **instructors:** id, name
#  - **sections:** uuid, course_offering_uuid,room_uuid, schedule_uuid
#  - **teachings:** instructor_id, section_uuid
#  
#  **Source:** https://www.kaggle.com/Madgrades/uw-madison-course
"""
path = "Datasets/uw-madison-courses/"

df_list = []
for filename in os.listdir(path):
    if filename.endswith(".csv"):
        filename_list = filename.split(".")  # separate path from .csv
        df_name = filename_list[0]
        df = spark.read.csv(path + filename, inferSchema=True, header=True)
        df.name = df_name
        df_list.append(df_name)
        exec(df_name + ' = df')
##
#
print("Full list of dfs:")
print(df_list)

rooms.show()
sections.show(4)

step1 = teachings.join(instructors, teachings.instructor_id == instructors.id, how='left').select(['instructor_id','name','section_uuid'])
step1.limit(4).show(5)

step2 = step1.join(sections, step1.section_uuid == sections.uuid, how='left').select(['name','course_offering_uuid'])
step2.limit(4).show()

step3 = step2.withColumnRenamed('name', 'instructor').join(course_offerings, step2.course_offering_uuid == course_offerings.uuid, how='inner').select(['instructor','name','course_offering_uuid'])
step3.show(4)