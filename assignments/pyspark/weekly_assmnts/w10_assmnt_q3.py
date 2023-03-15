from pyspark import SparkContext 

sc = SparkContext("local", "Wordcount app") 

lines = sc.textFile("/mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/Ft_Bigdata/Ft_dataengg_bootcamp/labs/datasets/words.txt")
counts = lines.flatMap(lambda x: x.split(' ')) \
			  .map(lambda x: (x, 1)) \
			  .reduceByKey(lambda a, b: a + b)
output = counts.sortBy(lambda x: x[1],ascending=False)

# print(output.collect())

for (word, count) in output.collect()[:10]:
	print("%s: %i" % (word, count))