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
hdfs dfs -head  /user/training/weather/weather_data.txt

lines=`wc -l /mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/futurense_hadoop-pyspark/labs/dataset/weather/weather_data.txt`

line=`echo $lines | cut -d' ' -f1`

h1=`expr $line / 2`
h2=`expr $line - $h1`

# cd /mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/futurense_hadoop-pyspark/labs/dataset/weather/

# touch weather1.txt
# touch weather2.txt
# touch weather3.txt

cat /mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/futurense_hadoop-pyspark/labs/dataset/weather/weather_data.txt | head -$h1 > weather1.txt
cat /mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/futurense_hadoop-pyspark/labs/dataset/weather/weather_data.txt | tail -$h2 > weather2.txt

cat weather1.txt weather2.txt > weather3.txt

