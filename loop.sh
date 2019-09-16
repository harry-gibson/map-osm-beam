#!/bin/bash
input=$1
while IFS= read -r line
do
 echo "$line"
 A="$(echo ${line} | cut -d"/" -f3)"
 sh geofabrik.sh $line $A
done < "$input"