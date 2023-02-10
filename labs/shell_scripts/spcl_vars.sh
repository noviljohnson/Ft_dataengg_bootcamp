#!/bin/bash

#**************************************
# Shows the usage of special variables
#**************************************


echo "file name is $0"
echo "first arg is $1"

echo "total number of values passed  : "$#
echo "all values passed :              "$*
echo "home directory :                 "$HOME

echo "all args :                       "$@
echo "exit status of last command :    "$?

echo "process num of current shell :   "$$
