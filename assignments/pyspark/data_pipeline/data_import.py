print('file 1 started')
from pyspark.sql import SparkSession
from datetime import datetime

data_path = '/mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/Ft_Bigdata/Ft_dataengg_bootcamp/labs/datasets/bankmarketdata.csv'
dir_path = '/mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/Ft_Bigdata/Ft_dataengg_bootcamp/assignments/pyspark/data_pipeline'

with open(dir_path+'/import_log.txt','w+') as file:
    file.write("\n File paths \n"+data_path+"\n"+dir_path+"\n")
    file.write(datetime.now().strftime("%d-%m-%Y   %H:%M:%S"))

    spark = SparkSession.builder.master("local").appName('bankdataLoadApp').getOrCreate()
    file.write("\n Spark Session \n")
    file.write("\n " + str(spark) + "\n")

    bank_df = spark.read.csv(path=data_path,header=True,sep=';',inferSchema=True)
    file.write("\n Data read from bankmarket csv \n")

    bank_df.write.mode('append').parquet(dir_path+'/output_1')
    file.write("\n Data stored as parquet format \n")
