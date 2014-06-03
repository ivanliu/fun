#!/bin/bash
# 
# Run something with a given time frame in yyyyMMddHH format
# 

# handle command line parameters
command=$1
start_from=$2
end_by=$3

# convert to unix time
curr_ts=`date -d "${start_from:0:8} ${start_from:8:2}" +"%s"`
end_ts=`date -d "${end_by:0:8} ${end_by:8:2}" +"%s"`

# run command with yyyyMMddHH
while [ $curr_ts -le $end_ts ]
do
    curr_str=`date -d @"$curr_ts" +"%Y%m%d%H"`
    #ret=`$command $curr_str`
    $command $curr_str
    if [ "$?" == "0" ]; then
        echo "Succ: $curr_str"
    else
        echo "Fail: $curr_str"
    fi

    curr_ts=$(( $curr_ts + 3600))
done

