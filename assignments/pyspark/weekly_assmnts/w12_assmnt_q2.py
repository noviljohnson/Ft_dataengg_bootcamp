from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


spark = SparkSession \
    .builder \
    .appName("Movie Streaming") \
    .getOrCreate()

schema = StructType([ StructField("movieId", StringType(), True),
                      StructField("title", StringType(), True),
                      StructField("genres", StringType(), True),
                      StructField("Yor", IntegerType(), True),
                      StructField("time_stamp", TimestampType(), True)])

df = spark\
    .readStream\
    .schema(schema)\
    .json('hdfs://localhost:9000/user/training/movie/movies1.json')


newDF=df.select('title','Yor','genres').withColumn('new_genre',split(df.genres, '[|]')).show(3)

groupDF = newDF.withWatermark("time_stamp", "60 seconds") \
    .groupBy( \
    window(newDF.time_stamp, "5 minutes", "60 seconds"),
    newDF['new_genre']
).count().orderBy('window')


#a
query1 = (
  groupDF
    .writeStream
    .format("console")
    .outputMode("complete")
    .start()
)

#b

groupDF1 = newDF.withWatermark("time_stamp", "5 minutes") \
    .groupBy( \
    window(newDF.time_stamp, "5 minutes", "60 seconds"),
    newDF['new_genre']
).count().max().orderBy('window')

query2 = (
  groupDF1
    .writeStream
    .format("console")
    .outputMode("complete")
  .start()
)

#c