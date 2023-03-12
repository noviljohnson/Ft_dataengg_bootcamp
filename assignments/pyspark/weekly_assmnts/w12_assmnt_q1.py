import pyspark

from pyspark.sql import SparkSession

from pyspark.sql.functions import *
from pyspark.sql import Window
from datetime import datetime

spark = SparkSession.builder.master('local').appName('AssignmentApp').getOrCreate()


# a.
movies_df = spark.read.csv("/mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/Ft_Bigdata/Ft_dataengg_bootcamp/labs/datasets/new_movies_ds/movies.csv",header=True,inferSchema=True,sep=',')

get_yor = udf(lambda x : x.split(' ')[-1][1:5])

print(movies_df.printSchema())

df_movies = movies_df.withColumn('Yor', get_yor(movies_df.title)).withColumn('time_stamp', lit(datetime.now()))

print(df_movies.show())

df_movies.write.mode('overwrite').save('hdfs://localhost:9000/user/training/movie/movies1.csv')


# b.

df_movies.toJSON().saveAsTextFile('hdfs://localhost:9000/user/training/movie/movies1.json')