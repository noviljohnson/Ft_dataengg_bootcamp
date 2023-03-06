import pyspark

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName('bankdataLoadApp').getOrCreate()


Path = '/mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/Ft_Bigdata/Ft_dataengg_bootcamp/labs/datasets/bankmarketdata.csv'
bank_df = spark.read.csv(path=Path,header=True,sep=';',inferSchema=True)


bank_df.write.parquet('/mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/Ft_Bigdata/Ft_dataengg_bootcamp/assignments/pyspark/data_pipeline/output_1')