from pyspark import SparkContext 

sc = SparkContext("local", "Movies app") 

# 2
movies = sc.textFile("hdfs://localhost:9000/user/training/movie/movies1.csv")

# a
print("total Movies : ", movies.count())

# b
year_wise_moves = movies.flatMap(lambda x: [x.split(',')]).map(lambda x: (x[-1],1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[1],ascending=False)
print(year_wise_moves.collect())

# c
genre = movies.map(lambda x: (x.split(',')[3],x.split(',')[2]))

genre.groupByKey().mapValues(len).collect()

