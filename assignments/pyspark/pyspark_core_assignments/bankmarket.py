# Assignment #2
# 	Bank Marketing Campaign Data Analysis with RDD API
# 	a) Load Bank Marketing Dataset and create RDD		
# 	b) Give marketing success rate. (No. of people subscribed / total no. of entries)
# 	c) Give marketing failure rate
# 	d) Maximum, Mean, and Minimum age of the average targeted customer
# 	e) Check the quality of customers by checking the average balance, median balance of customers
# 	f) Check if age matters in marketing subscription for deposit
# 	g) Show AgeGroup [Teenagers, Youngsters, MiddleAgers, Seniors] wise Subscription Count.
# 	h) Check if marital status mattered for subscription to deposit.
# 	i) Check if age and marital status together mattered for subscription to deposit scheme

from pyspark import SparkContext 

sc = SparkContext("local", "Weather app") 

# a.
bank_rdd = sc.textFile("/mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/Ft_Bigdata/Ft_dataengg_bootcamp/labs/datasets/bankmarketdata.csv")

# b.
data = bank_rdd.collect()
cols = data[0]
rows = data[1:]

rows_rdd = sc.parallelize(rows)

# subscribers_count = rows_rdd.flatMap(lambda x: [x.split(';')]).\
#         map(lambda x: (x[-1],1)).\
#         reduceByKey(lambda x,y: x+y).\
#         collect()[0][1]

subscribers_count = rows_rdd.filter(lambda x: 'yes' in  x.split(';')[-1]).count()
total_rows = rows_rdd.count()

success_rate = subscribers_count / total_rows * 100

print("Success Rate : ", success_rate)


# c.
failure_rate = 100 - success_rate


# d.
# 	d) Maximum, Mean, and Minimum age of the average targeted customer
minimum = rows_rdd.filter(lambda x: 'yes' in  x.split(';')[-1]).\
    map(lambda x: int(x.split(";")[0])).min()
avrage = rows_rdd.filter(lambda x: 'yes' in  x.split(';')[-1]).\
    map(lambda x: int(x.split(";")[0])).mean()
maximum = rows_rdd.filter(lambda x: 'yes' in  x.split(';')[-1]).\
    map(lambda x: int(x.split(";")[0])).max()


# 	e) Check the quality of customers by checking the average balance, median balance of customers
mean_bal = rows_rdd.map(lambda x: int(x.split(";")[5])).mean()
median_bal = rows_rdd.map(lambda x: int(x.split(";")[5])).\
        sortBy(lambda x: x).collect()[total_rows//2]


# 	f) Check if age matters in marketing subscription for deposit
age_wise_count = rows_rdd.filter(lambda x: 'yes' in x.split(";")[-1]).\
        map(lambda x: (x.split(';')[0],1)).\
        reduceByKey(lambda x,y: x+y).\
        sortBy(lambda x: x[0]).collect()
for i,val in age_wise_count:
    print(i,val)


# 	g) Show AgeGroup [Teenagers, Youngsters, MiddleAgers, Seniors] wise Subscription Count.
age_cat = {0:"Teen",20:"Young",40:"MiddleAgers",60:"Seniors",80:"Seniors"}

age_count = sc.parallelize(age_wise_count)
age_cat_count = age_count.map(lambda x: ((int(x[0])//20)*20,x[1])).\
    map(lambda x: (age_cat[x[0]],x[1])).\
    reduceByKey(lambda x,y: x+y).collect()

print(age_cat_count)


# 	h) Check if marital status mattered for subscription to deposit.

# rows_rdd.map(lambda x: (x.split(";")[2].strip('"'),x.split(';')[5])).take(3)


# 	i) Check if age and marital status together mattered for subscription to deposit scheme
