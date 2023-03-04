#!/bin/bash

echo "********* Started Execution *************"

echo "executing data_import.py"
spark-submit data_import.py

if [ $? -eq 0 ]
then
    echo "executing data_cleaning.py"
    spark-submit data_cleaning.py
    if [ $? -eq 0 ]
    then 
        echo "executing  data_transform.py"
        spark-submit --jars /mnt/c/Users/miles.MILE-BL-4766-LA.000/Downloads/mysql-connector-j-8.0.32/mysql-connector-j-8.0.32.jar  data_transform.py
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

