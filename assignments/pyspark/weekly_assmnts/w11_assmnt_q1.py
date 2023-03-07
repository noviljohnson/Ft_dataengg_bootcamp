from pyspark import SparkContext 

sc = SparkContext("local", "Movies app") 

# 1.a
lines = sc.textFile("/mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/Ft_Bigdata/Ft_dataengg_bootcamp/labs/datasets/new_movies_ds/movies.csv")


mov1 =  lines.map(lambda x: str(x + "," + str(x.split(',')[1][::-1][1:5][::-1])))

mov1.saveAsTextFile('hdfs://localhost:9000/user/training/movie/movies1.csv')


# 1.b

ratings_rdd = sc.textFile('/labs/dataset/ratings.csv')

ratings_rdd.saveAsTextFile('hdfs://localhost:9000/user/training/movie/ratings.csv')
