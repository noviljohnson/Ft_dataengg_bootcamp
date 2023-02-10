#Hashbang / Shebang


#!/bin/bash


# create directory "assignments" under /home/ubuntu/shell_scripts
# create file "cities.txt'
# add city names into the file
# filter city name contains 'New'
# replace 'New' with 'Old'
# write the output to 'old_cities.txt'


mkdir assignments
echo 'Directory is created'

touch assignments/cities.txt
echo 'cities.txt is created'

echo "Bangalore" >> assignments/cities.txt
echo "Vijayawada" >> assigments/cities.txt
echo "Hydrabad" >> assignments/cities.txt
echo "Chennai" >> assignments/cities.txt
echo 'Cities are added to the file'

cat assignments/cities.txt


echo 'filtering cities with e'
grep -i 'e' assignments/cities.txt

cat assignments/cities.txt | sed 's/e/EE/g' > assignments/ch_cities.txt
echo 'Replaced e with EE and written to new file ch_cities.txt'

cat assignments/ch_cities.txt

echo "all done"
