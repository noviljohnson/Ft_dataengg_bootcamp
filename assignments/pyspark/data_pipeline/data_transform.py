
import pandas as pd
import pyspark 

from pyspark.sql.functions import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.config("spark.jars", "/mnt/c/Users/miles.MILE-BL-4766-LA.000/Downloads/mysql-connector-j-8.0.32/mysql-connector-j-8.0.32.jar") \
    .master("local").appName("bankMarketData_Transform").getOrCreate()

# spark = SparkSession.builder.master("local").appName('bankdataClean').getOrCreate()

df_bnk = spark.read.parquet("/mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/Ft_Bigdata/Ft_dataengg_bootcamp/assignments/pyspark/data_pipeline/output_2/")

df_groups = df_bnk.select('age','y').filter(df_bnk.y == 'yes').withColumn('age_groups',concat(lit('Below '),ceil(df_bnk.age/5)*5, lit(' Years'))).groupBy('age_groups').count().orderBy('age_groups')

# spark.sql('')

df_groups.write.mode('append') \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/pyspark_training") \
	.option("driver", "com.mysql.jdbc.Driver") \
    .option("dbtable", "age_groups") \
    .option("user", "pyspark") \
    .option("password", "pyspark") \
    .save()