#!/bin/bash

#****************
# Q - 3
#***************

mkdir demo
cd deom/
touch file1.txt
echo "hello Futurense" >> file1.txt

touch .file2.txt
echo "Bye Futurense" >> .file2.txt

ls -la

#*****************
# Q - 4
#*****************

a=1
while [ $a -eq 1 ]
do
	echo "choose an operation : "
	echo "press 1 for addition : "
	echo "press 2 for subtraction : "
	echo "press 3 for multiplication : "
	echo "press 4 for division : "
	echo "press 0 to exit : "
	echo
	
	read ch
	
	if [ $ch -eq 0 ]
	then
		echo " ****** Exit ******"
		break
	fi

	echo "enter input x: "
	read x
	echo "enter input y: "
	read y
	
	echo
	
	if [ $ch -eq 1 ] 
	then
		echo "the addition of $x and $y is : " `expr $x + $y`
	
	elif [ $ch -eq 2 ] 
	then
		echo "the subtraction of $x and $y is : " `expr $x - $y`
	
	elif [ $ch -eq 3 ] 
	then
		echo "the multiplication of $x and $y is : " `expr $x \* $y`
	
	elif [ $ch -eq 4 ] 
	then
		echo "the division of $x and $y is : " `expr $x / $y`
	
	else 
		echo "****** Entered wrong option *********"
		echo "*****  There is no $ch option *******"
		echo "**** Please select correct option ***"
	fi

	echo
done	


#*****************
# Q - 2
#*****************


#!/bin/bash

cd '/mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/Ft_Bigdata/Ft_dataengg_bootcamp/assignments/shell_scripts/Desktop'

tar -xzf ../testData.tar testData

# tar -xzf '/mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/Ft_Bigdata/Ft_dataengg_bootcamp/assignments/shell_scripts/testData.tar' testData

cat testData/data.txt | head -c 5


#********************
# Q - 1
#********************


ls -R

echo

# removing all files with .txt 
rm *.txt



