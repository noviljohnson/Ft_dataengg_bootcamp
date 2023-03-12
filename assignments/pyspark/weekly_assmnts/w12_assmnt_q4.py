# ========== Kafka creating topic and launch Producer ==================
# 1: Start Producer
# kafka-console-producer.sh --broker-list localhost:9092 --topic movies

# kafka-console-producer.sh --broker-list localhost:9092 --topic movies-stream-analysis-out
# kafka-console-producer.sh --broker-list localhost:9092 --topic moives < hdfs://localhost:9000/user/training/movie/movies.json


from pkg_resources._vendor.pyparsing import col

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

#Create Spark Session
spark = SparkSession \
    .builder \
    .appName("movieAnalysis") \
    .getOrCreate()

#Consume message from Kafka topic and create Dataset
moviesStreamingDF = spark.readStream.format("kafka")\
	                .option("kafka.bootstrap.servers", "localhost:9092")\
                    .option("subscribe", "movies")\
                    .option("kafka.group.id", "movies-consumer")\
	                .option("startingOffsets", "earliest")\
	                .load()

#Print Schema
moviesStreamingDF.printSchema()

#Schema definition for consuming message
schema = StructType([ StructField("movieId", IntegerType(), True),
                      StructField("title", StringType(), True),
                      StructField("genres", StringType(), True),
                      StructField("YoR", IntegerType(), True),
			    StructField('time_stamp', TimestampType(), True)])

#Converting received kafka binary message to json message applying custom schema
moviesStreamingDF = moviesStreamingDF.select(from_json(col("value").cast("String"), schema).alias("movies"))

#Print Schema
moviesStreamingDF.printSchema()

moviesStreamingDF = moviesStreamingDF.select("movies.movieId","movies.title","movies.genres","movies.YoR","movies.time_stamp")


# Handling the threshold
trendingDF = moviesStreamingDF.withWatermark("time", "10 minutes") \
    .groupBy( \
    window(moviesStreamingDF.time, "5 minutes", "1 minute"),
    moviesStreamingDF.status
).count().orderBy('window')


# write output ot kafka

query = trendingDF\
    .selectExpr("CAST(genre AS STRING) AS key", "CAST(count AS STRING) AS value") \
	.writeStream \
	.outputMode("complete") \
	.format("kafka") \
	.option("kafka.bootstrap.servers","localhost:9092") \
	.option("topic","movies-stream-analysis-out") \
	.option("checkpointLocation", "movies") \
	.start() \
	.awaitTermination()