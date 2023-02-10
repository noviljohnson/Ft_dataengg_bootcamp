#!/bin/sh

#************************
# Loops
#************************


# while
a=0
while [ "$a" -lt 10 ]
do 
	b="$a"
	echo "value of b : $b"
	
	a=`expr $a + 1`
done


# until
a=15
until [ "$a" -lt 10 ]
do
	echo " value of a : $a"
	a=`expr $a - 1`
done


