

from pyspark import SparkContext 

sc = SparkContext("local", "Weather app") 

lines = sc.textFile("/mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/Ft_Bigdata/Ft_dataengg_bootcamp/labs/datasets/weather_data.txt")

max_temp = lines.flatMap(lambda x: [x.split()[5]]) \
            .map(lambda x: ('Max_temp',float(x))) \
            .reduceByKey(lambda a, b:max(a,b))
output = max_temp.collect()
for (word, count) in output:
    print(word,count)

mim_temp = lines.flatMap(lambda x: [x.split()[6]]) \
            .map(lambda x: ('Min_temp',float(x))) \
            .reduceByKey(lambda a, b:max(a,b))
output = mim_temp.collect()
for (word, count) in output:
    print(word,count)