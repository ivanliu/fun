#!/bin/bash
# 
# Compare strings by ignoring case
# 

# handle command line parameters
str1=$1
str2=$2

shopt -s nocasematch

case "$str1" in
    $str2 ) echo "match";;
    *) echo "no match";;
esac

shopt -u nocasematch

