#!/bin/bash

# Assignment #1:
# 	Create directory "weather" under /user/training
# 	Load the data from ~/futurence_hadoop-pyspark/labs/dataset/weather to /user/training/weather
# 	Display the weather data
# 	Split the weather data file and store as weather1 and wearther2
# 	Merge the weather data file as weather3
# 	Display the weather stats



# hdfs dfs -mkdir /user/training/weather
hdfs dfs -put /mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/futurense_hadoop-pyspark/labs/dataset/weather/weather_data.txt  /user/training/weather
hdfs dfs -cat /user/training/wearther/weather_data.txt

lines=`wc -l /user/training/weather/weather_data.txt`


hdfs dfs -appendtoFile -head  /user/training/weather
