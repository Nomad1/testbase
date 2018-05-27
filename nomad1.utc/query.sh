#!/bin/bash

db="test.db"

ip=$1
if [ -z "$ip" ]; then
    echo "IP is empty"
    exit
fi
cpu=$2
if [ -z "$cpu" ]; then
    echo "CPU is empty"
    exit
fi

from=$3
if [ -z "$from" ]; then
    echo "time_start is empty"
    exit
fi

to=$4
if [ -z "$to" ]; then
    echo "time_end is empty"
    exit
fi

startTs=$(date -j -u -f "%Y-%m-%d %H:%M" "$from" "+%s")
endTs=$(date -j -u -f "%Y-%m-%d %H:%M" "$to" "+%s")


query="SELECT Timestamp, Load FROM data WHERE ip = \"$1\" AND cpu=$cpu AND timestamp > $startTs - 59 AND timestamp <= $endTs;"

result=(`echo -e $query | sqlite3 -batch $db`)

for line in ${result[*]}
do
  data=(${line//|/ })
  date=$(date -j -u -f "%s" "${data[0]}" +"%Y-%m-%d %H:%M")
  printf "($date, ${data[1]}%%), "
done