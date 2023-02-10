#!/bin/sh

#***************************************
# Operators 
#***************************************


# logical operators
# arthematic operators
# bitwise operators
# relational operators
# boolean operators
# string operators

val=`expr 2 + 2`

arg1=$1
arg2=$2

sum=`expr $arg1 + $arg2`
echo "sum of 2, 2 : $val"
echo "sum of args : $sum"

echo "sum : " `expr $arg1 + $arg2`
echo "mul : " `expr $arg1 \* $arg2`
echo "div : " `expr $arg1 / $arg2`
echo "sub : " `expr $arg1 - $arg2`
echo "gt : " `expr $arg1 \> $arg2`
echo "lt : " `expr $arg1 \< $arg2`
echo "neq : " `expr $arg1 \!\= $arg2`
echo "eq : " `expr $arg1 \== $arg2`

echo "eq : " [$arg1 \== $arg2] 

 
