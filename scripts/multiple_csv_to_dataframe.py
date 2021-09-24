# read multiple .csv file and create a dataframe

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

