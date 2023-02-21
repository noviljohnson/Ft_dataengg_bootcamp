from pyspark import SparkContext 

sc = SparkContext("local", "Wordcount app") 

lines = sc.textFile("/home/ubuntu/futurense_hadoop-pyspark/labs/dataset/wordcount/wordcount-input.txt")
counts = lines.flatMap(lambda x: x.split(' ')) \
			  .map(lambda x: (x, 1)) \
			  .reduceByKey(lambda a, b: a + b)
output = counts.collect()
for (word, count) in output:
	print("%s: %i" % (word, count))
