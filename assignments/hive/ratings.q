CREATE TABLE IF NOT EXISTS RATINGS (userid int, movieid int, rating float,timestmp date )
    COMMENT 'Movie ratings'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    STORED AS TEXTFILE;

    
LOAD DATA LOCAL INPATH '/mnt/c/Users/miles/Downloads/ratings.csv' OVERWRITE INTO TABLE ratings;


------------------------------

--hive query to find each category of ratings

SELECT rating,COUNT(movieid) AS ratings_count
FROM ratings
GROUP BY rating;


# sudo service ssh start
# ssh localhost
# start-dfs.sh
# start-yarn.sh 
# jps
# !connect jdbc:hive2:// username 