from pyspark import SparkContext

sc = SparkContext("local", "ratings app")

l1 = sc.textFile("/mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/Ft_Bigdata/Ft_dataengg_bootcamp/labs/datasets/ratings.csv")
for rating,vals in l1.map(lambda x:x.split(',')).groupBy(lambda x:x[2]).collect():
    if rating!="rating":
        print(rating,len(vals))