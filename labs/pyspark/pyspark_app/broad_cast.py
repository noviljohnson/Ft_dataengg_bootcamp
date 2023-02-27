from pyspark import SparkContext 
sc = SparkContext("local", "Broadcast app") 
words_new = sc.broadcast(["scala", "java", "hadoop", "spark", "akka"]) 
data = words_new.value 
print("Words new : ",words_new._value)
for i in words_new.value:
    print(i)
print("Stored data -> %s" % (data)) 
elem = words_new.value[2] 
print("Printing a particular element in RDD -> %s" % (elem))