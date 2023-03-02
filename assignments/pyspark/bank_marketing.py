

import pandas as pd
import pyspark 

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark import SparkContext

spark = SparkSession.builder.appName('pyspark_bank_marketing').getOrCreate()

df = spark.read.csv("/mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/Ft_Bigdata/Ft_dataengg_bootcamp/labs/datasets/bankmarketdata.csv",sep=';',inferSchema=True,header=True)


# Give marketing success rate.(No. of people subscribed / total no. of entries)
df.filter(df.y=='yes').count()/df.count()*100
spark.sql("select sum(if(y=='yes',1,0))*100/count(*) as successRate from bankmarketing").show()

# category wise num of subscribers

spark.sql("select sum(if(y=='yes',1,0)) as count, \
case when age < 13 then 'kids' \
when age < 20 then 'teenagers' \
when age < 31 then 'young' \
when age < 50 then 'middle' \
else 'Seniors' \
end \
as cat from bankmarketing where age is not null group by cat").show()
# +-----+---------+
# |count|      cat|
# +-----+---------+
# | 1385|  Seniors|
# | 1127|    young|
# |   18|teenagers|
# | 2759|   middle|
# +-----+---------+


# Maximum, Mean, and Minimum age of the average targeted customer
df.agg(min('age'),avg('age'),max('age')).show()


# 5. Check the quality of customers by checking the average balance, median balance of customers
df.agg(avg('balance'),min('balance'),max('balance')).withColumn('med',lit(df.approxQuantile('balance',[0.5],0.1)[0])).show()

from pyspark.ml.feature import Bucketizer

buckets = Bucketizer(splits=[0,19,40,50,float('Inf')],inputCol='age', outputCol='bins')
df_bins = buckets.setHandleInvalid('keep').transform(df)

t = {0.0:"infant", 1.0: "minor", 2.0:"adult", 3.0: "senior"}
udf_foo = udf(lambda x: t[x], StringType())
df_bins.withColumn("age_bucket", udf_foo("buckets")).show()

# 6. Check if age matters in marketing subscription for deposit


# 7. Check if marital status mattered for subscription to deposit.

# 8. Check if age and marital status together mattered for subscription to deposit scheme

df.approxQuantile('count',[0.5],0.1)[0]
df.approxQuantile()
df.sampleBy()
df.sampleBy(col("key"), fractions={2: 1.0}, seed=0).count()
# 33

df.sample()

sampled = df.sampleBy("age", fractions={0: 0.1, 1: 0.2}, seed=0)
sampled.groupBy("key").count().orderBy("key").show()
# +---+-----+
# |key|count|
# +---+-----+
# |  0|    3|
# |  1|    6|
# +---+-----+

