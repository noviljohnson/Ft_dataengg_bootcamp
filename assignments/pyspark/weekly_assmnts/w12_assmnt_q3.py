from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import window

# Path to our loan JSON files
inputPath = "hdfs://localhost:9000/user/training/movie/movies1.json"

spark = SparkSession \
    .builder \
    .appName("LoanStreamingAnalysis") \
    .getOrCreate()

# Explicitly set schema
schema = StructType([ StructField("Id", IntegerType(), True),
                      StructField("Title", StringType(), True),
                      StructField("genres", StringType(), True),
                      StructField("Yor", StringType(), True),
                      StructField("time_stamp", TimestampType(), True)])

# Create streaming data source
movie_Df = (
  spark
    .readStream
    .schema(schema)
    .option("maxFilesPerTrigger", 1)
    .json(inputPath)
)


# Group the data by window and status and compute the count of each group
genreWiseNoMoviesWindow = movie_Df.withWatermark("time_stamp", "60 seconds") \
    .groupBy( \
    window(movie_Df.time_stamp, "10 seconds", "5 seconds"),
    movie_Df.gen
).count().orderBy('window')


# Stream output to console
query = (
  genreWiseNoMoviesWindow
    .writeStream
    .format("console")
    .outputMode("complete")
    .start()
)

query.awaitTermination()