# Assignment #3

# 	Bank Marketing Campaign Data Analysis with PySpark SQL

# 	Create PySpark Application - bank-marketing-analysis.py. Perform below operations.
# 	a) Load Bank Marketing Campaign Data from csv file
# 	b) Get AgeGroup wise SubscriptionCount
# 	c) Write the output in parquet file format
# 	d) Load the data from parquet file written above
# 	e) Show the data
# 	f) Filter AgeGroup with SubcriptionCount > 2000 and write into Avro file format
# 	g) Load the data from avro file written above
# 	h) Show the data

# 	1] spark-submit bank-marketing-analysis.py => Runs in local mode
# 	2] Run in cluster mode
# 	3] Schedule to PySparkApplication to run every N mins

from pyspark.sql.functions import *
from pyspark.sql.types import *

from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.jars", "/mnt/c/Users/miles.MILE-BL-4766-LA.000/Downloads/mysql-connector-j-8.0.32/mysql-connector-j-8.0.32.jar") \
    .master("local").appName("PySpark_MySQL_test").getOrCreate()

# a.

df_bank = df = spark.read.csv("/mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/Ft_Bigdata/Ft_dataengg_bootcamp/labs/datasets/bankmarketdata.csv",header=True,inferSchema=True,sep=';')

# b.

age_cat_dict = {20:'Teen',40:'Young',60:'Middle',80:'Seniors',100:'Seniors'}
age_cat_udf = udf(lambda age: age_cat_dict[age],StringType())
df_age_cat = df_bank.select('age','y').\
            filter(col('y') == 'yes').\
            withColumn('age_cat',age_cat_udf(ceil(df_bank['age']/20)*20)).\
            groupBy('age_cat').count()
print(df_age_cat.show())

# 	c) Write the output in parquet file format

df_age_cat.write.mode('append').parquet('/mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/Ft_Bigdata/Ft_dataengg_bootcamp/assignments/pyspark/core_assignments/ageGroup_wise_count')


# 	d) Load the data from parquet file written above

df_ageGrop = spark.read.parquet('/mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/Ft_Bigdata/Ft_dataengg_bootcamp/assignments/pyspark/core_assignments/ageGroup_wise_count')

# 	e) Show the data
df_ageGrop.show()

# 	f) Filter AgeGroup with SubcriptionCount > 2000 and write into Avro file format

# --packages org.apache.spark:spark-avro_2.12:3.3.2
df_ageGrop.filter(col('count') > 2000).write.mode('append').format('avro').save('/mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/Ft_Bigdata/Ft_dataengg_bootcamp/assignments/pyspark/core_assignments/ageGroup_wise_avro')

# 	g) Load the data from avro file written above
# 	h) Show the data

df_countGt2000 = spark.read.format('avro').load('/mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/Ft_Bigdata/Ft_dataengg_bootcamp/assignments/pyspark/core_assignments/ageGroup_wise_avro')
print(df_countGt2000.show())



# master - <master-ip>
# *******************
# Running spark in cluster mode.
# 1.) First lauch master and slave nodes
# 		start-master.sh
# 		start-slave.sh
# 2.) Find the master IP (You can find master IP at localhost:8080) and mention it in the spark-submit
# 		spark-submit --master spark://Rakesh.localdomain:7077 --packages org.apache.spark:spark-avro_2.12:3.3.2 Assignment_3.py 