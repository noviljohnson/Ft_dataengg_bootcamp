import pyspark

from pyspark.sql import SparkSession

from pyspark.sql.functions import *
from pyspark.sql import Window



spark = SparkSession.builder.master('local').appName('AssignmentApp').getOrCreate()

movies_df = spark.read.csv('hdfs://localhost:9000/user/training/movie/movies1.csv',header=True,inferSchema=True)

ratings_df = spark.read.csv('hdfs://localhost:9000/user/training/movie/ratings.csv',header=True,inferSchema=True)


movies_df.createOrReplaceTempView('movies')
ratings_df.createOrReplaceTempView('ratings')
# a.)
spark.sql("select YoR, count(title) as count from movies group by YoR order by count desc").show()


# b.)
spark.sql("select m.title,m.genre,r.rating from movies m inner join ratings r on m.movieId = r.movieId where m.YoR=2021 order by r.rating desc limit 3").show()

# c.)

movie_grp = movies_df.groupBy('YoR','genre').agg(count('title').alias('count'))
window_spec = Window.partitionBy(['YoR','genre']).orderBy(movie_grp['count'].desc())

dense_rank_df = movie_grp.withColumn("dense_rank",dense_rank().over(window_spec))

dense_rank_df.filter(col('dense_rank')==1).show()