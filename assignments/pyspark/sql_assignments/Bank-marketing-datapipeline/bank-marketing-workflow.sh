#!/bin/bash


# Create Shell Script - bank-marketing-workflow.sh. Performa below operations.
# 	a) Create a workflow to sequentially execute Data Loading, Validation, Tranformation and Export jobs
# 	b) Schedule to run this workflow every N mins e.g. 15 mins and process the new input dataset if any


alias spark-submit='/opt/spark/bin/spark-submit'

local_path="/mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/Ft_Bigdata/Ft_dataengg_bootcamp/assignments/pyspark/sql_assignments/Bank-marketing-datapipeline"

cd "$local_path"

echo "*** Started Execution *****"

echo "executing bank-marketing-data-loading.py"
spark-submit bank-marketing-data-loading.py >> logs/exe_logs.txt 2>> "logs/loading_logs.txt"

if [ $? -eq 0 ]
then
    echo "executing  bank-marketing-validation.py"
    spark-submit bank-marketing-validation.py >> logs/exe_logs.txt 2>> "logs/validation_logs.txt"
    if [ $? -eq 0 ]
    then
        echo "executing  bank-marketing-tranformation.py"
        spark-submit \
            --packages org.apache.spark:spark-avro_2.12:3.3.2 \
            --jars /mnt/c/Users/miles.MILE-BL-4766-LA.000/Downloads/mysql-connector-j-8.0.32/mysql-connector-j-8.0.32.jar \
             bank-marketing-tranformation.py >> logs/exe_logs.txt 2>> "logs/transformation_logs.txt"
        if [ $? -eq 0 ]
        then
            echo "executing  bank-marketing-export.py"
            spark-submit --jars /mnt/c/Users/miles.MILE-BL-4766-LA.000/Downloads/mysql-connector-j-8.0.32/mysql-connector-j-8.0.32.jar \
                        --packages org.apache.spark:spark-avro_2.12:3.3.2 \
                        bank-marketing-export.py >> logs/exe_logs.txt 2>> "logs/export_logs.txt"
            if [ $? -eq 0 ]
            then
                echo "___________ All Jobs Done____________"
            else
                echo "============Error in Export==========="
            fi
        else 
            echo "==========Error in Transformation===========  "
        fi
    else 
        echo "===============Error in validation=============== "
    fi
else 
    echo "================Error in Loading ================"
fi


