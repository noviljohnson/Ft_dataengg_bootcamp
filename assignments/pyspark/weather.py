

from pyspark import SparkContext 

sc = SparkContext("local", "Weather app") 

lines = sc.textFile("/mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/Ft_Bigdata/Ft_dataengg_bootcamp/labs/datasets/weather_data.txt")

max_temp = lines.flatMap(lambda x: [x.split()[5]]) \
            .map(lambda x: ('Max_temp',float(x))) \
            .reduceByKey(lambda a, b:max(a,b))
output1 = max_temp.collect()

mim_temp = lines.flatMap(lambda x: [x.split()[6]]) \
            .map(lambda x: ('Min_temp',float(x))) \
            .reduceByKey(lambda a, b:min(a,b))
output = mim_temp.collect()

# max
max_month = lines.flatMap(lambda x: [[x.split()[1]]+[x.split()[5]]]).map(lambda x: (x[0][4:6],float(x[1]))).reduceByKey(lambda a,b: max(a,b))
max_month = max_month.collect()
# Max
min_month = lines.flatMap(lambda x: [[x.split()[1]]+[x.split()[6]]]).map(lambda x: (x[0][4:6],float(x[1]))).reduceByKey(lambda a,b: min(a,b))
min_month = min_month.collect()

print(output1[0][0], output1[0][1])
print(output[0][0], output[0][1])

print("month wise Maximum")
for (word, count) in min_month:
    print(word,count)

print("month wise Minimum")
for (word, count) in max_month:
    print(word,count)
