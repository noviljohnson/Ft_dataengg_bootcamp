
import pandas as pd
import pyspark 

from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark import SparkContext

sc = SparkContext("local", "weather app")


# spark.read('/mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/Ft_Bigdata/Ft_dataengg_bootcamp/labs/datasets/weather_data.txt')

lines = sc.textFile("/mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/Ft_Bigdata/Ft_dataengg_bootcamp/labs/datasets/weather_data.txt")


spark = SparkSession.builder.appName("pyspark-examples").getOrCreate()

lines1=lines.flatMap(lambda x: x.split() )#[x.split()[1],x.split()[5],x.split()[6]])
df = spark.createDataFrame(lines1,schema='_1 string, _2 float, _3 float')

df.withColumn('month',substring(df._2,5,2)).groupBy('month')