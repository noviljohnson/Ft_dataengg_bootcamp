#!/bin/bash

cd '/mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/Ft_Bigdata/Ft_dataengg_bootcamp/assignments/shell_scripts/Desktop'

tar -xzf ../testData.tar testData

# tar -xzf '/mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/Ft_Bigdata/Ft_dataengg_bootcamp/assignments/shell_scripts/testData.tar' testData

cat testData/data.txt | head -c 5 

