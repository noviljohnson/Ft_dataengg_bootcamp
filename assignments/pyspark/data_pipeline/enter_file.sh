#!/bin/bash

local_path="/mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/Ft_Bigdata/Ft_dataengg_bootcamp/assignments/pyspark/data_pipeline"

date > "$local_path/outputlogs.txt"
date
echo "********* Started Execution *************"

echo "executing data_import.py"
spark-submit data_import.py >> "/mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/Ft_Bigdata/Ft_dataengg_bootcamp/assignments/pyspark/data_pipeline/outputlogs.txt"

if [ $? -eq 0 ]
then
    echo "executing data_cleaning.py"
    
    spark-submit "$local_path/data_cleaning.py" >> "$local_path/outputlogs.txt"
    if [ $? -eq 0 ]
    then 
        # sudo service 
        echo "executing  data_transform.py"

        spark-submit --jars /mnt/c/Users/miles.MILE-BL-4766-LA.000/Downloads/mysql-connector-j-8.0.32/mysql-connector-j-8.0.32.jar  "$local_path/data_transform.py" >> "$local_path/outputlogs.txt"
        if [ $? -eq 0 ]
        then
            echo  "All Jobs done"
        else
            echo "============== ERROR in FILE 3 ==================="
        fi
    else
        echo "================ ERROR in FILE 2 ===================="
    fi 
else
    echo "================ ERROR in FILE 1 =================="
fi
echo "$?"

