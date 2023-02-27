from pyspark import SparkContext 
sc = SparkContext("local", "Cache app") 
words = sc.parallelize (
   ["scala", 
   "java", 
   "hadoop", 
   "spark", 
   "akka",
   "spark vs hadoop", 
   "pyspark",
   "pyspark and spark"]
) 
 
print(" "*100)
print(type(words))
print(type(words.cache()))

# copying an rdd into another var 
words_1 = words

# performing trs on words
words_2 = words.map(lambda x: (x,1)).reduceByKey(lambda a,b: a+b)
print(" "*100, "\n +++++++First Collect++++++++++")
print(words_2.collect())

cached_words = words_2.cache()

# now performing trs on words_1 
words_3 = words_1.map(lambda x: (x,1)).reduceByKey(lambda a,b: a*b)
print(" "*100, "\n +++++++Second Collect++++++++++")
print(words_3.collect())

# now performing trs on cached words
print(" "*100, "\n +++++++Third Collect++++++++++")
print(cached_words.collect())

caching = words.persist().is_cached 
print("Words got chached > %s" % (caching))