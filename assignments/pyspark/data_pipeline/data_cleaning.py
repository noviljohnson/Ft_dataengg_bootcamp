
# import pandas as pd
# import pyspark 
from datetime import datetime

from pyspark.sql.functions import *
from pyspark.sql import SparkSession
# from pyspark import SparkContext

# spark = SparkSession.builder.appName('Data_clean').getOrCreate()
spark = SparkSession.builder.master("local").appName('bankdataClean').getOrCreate()

df_bnk = spark.read.parquet('/mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/Ft_Bigdata/Ft_dataengg_bootcamp/assignments/pyspark/data_pipeline/output_1') #,sep=';',header=True)

with open('log.txt','w+') as file:
    file.write(datetime.now().strftime("%d-%m-%Y   %H:%M:%S"))
    file.write('Bank Market schema\n')
    file.write(str(df_bnk.columns)+'\n')


    file.write('Nulls in each column')
    for i in df_bnk.columns:
        file.write(str(i)+" : "+str(df_bnk.where(isnull(col(i))).count())+'\n')
df_age_nonNull = df_bnk.where(df_bnk.age.isNotNull())

# df_bnk.write.parquet("/mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/Ft_Bigdata/Ft_dataengg_bootcamp/assignments/pyspark/data_pipeline/output/bankmarket_cleaned.parquet")
df_age_nonNull.write.mode('append').parquet("/mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/Ft_Bigdata/Ft_dataengg_bootcamp/assignments/pyspark/data_pipeline/output_2/")
# df_bnk.write.mode('overwrite').parquet("/mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/Ft_Bigdata/Ft_dataengg_bootcamp/assignments/pyspark/data_pipeline/output/bankmarket_cleaned.parquet")
