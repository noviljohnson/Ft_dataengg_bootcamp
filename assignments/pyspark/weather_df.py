
import pandas as pd
import pyspark 

from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark import SparkContext

sc = SparkContext("local", "weather app")


# spark.read.load('/mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/Ft_Bigdata/Ft_dataengg_bootcamp/labs/datasets/weather_data.txt',format = 'csv')
# df_we.withColumn('_c0',split(regexp_replace(df_we._c0,"\s+",','),','))

lines = sc.textFile("/mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/Ft_Bigdata/Ft_dataengg_bootcamp/labs/datasets/weather_data.txt")

spark = SparkSession.builder.appName("pyspark-examples").getOrCreate()

lines1=lines.map(lambda x: x.split() )
df = spark.createDataFrame(lines1)

df_r = df.select('_2','_6','_7').withColumn('_6',df._6.cast('float')).withColumn('_7',df._7.cast('float'))

print(df_r.withColumn('month', substring(df_r._2,5,2)).groupBy('month').agg(min(df_r._7),max(df_r._6)).show())


# arrayStructureData = [
#         (("James","","Smith"),["Java","Scala","C++"],"OH","M",{"home":"City Center","work":"City Tech Park"}),
#         (("Anna","Rose",""),["Spark","Java","C++"],"NY","F",{}),
#         (("Julia","","Williams"),["CSharp","VB"],"OH","F",{}),
#         (("Maria","Anne","Jones"),["CSharp","VB"],"NY","M",{}),
#         (("Jen","Mary","Brown"),["CSharp","VB"],"NY","M",{}),
#         (("Mike","Mary","Williams"),["Python","VB"],"OH","M",{})
#         ]

# arrayStructureSchema = StructType([
#     StructField('name', StructType([
#             StructField('firstname', StringType(), True),
#             StructField('middlename', StringType(), True),
#             StructField('lastname', StringType(), True)
#             ])),
#     StructField('languages', ArrayType(StringType()), True),
#     StructField('state', StringType(), True),
#     StructField('gender', StringType(), True),
#     StructField('address', MapType(StringType(),StringType()), True)
# ])
