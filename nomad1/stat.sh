#!/bin/bash

db="test.db"

ip=$1
if [ -z "$ip" ]; then
    echo "IP is empty"
    exit
fi

from=$2
if [ -z "$from" ]; then
    echo "time_start is empty"
    exit
fi

to=$3
if [ -z "$to" ]; then
    echo "time_end is empty"
    exit
fi

startTs=$(date -j -f "%Y-%m-%d %H:%M" "$from" "+%s")
endTs=$(date -j -f "%Y-%m-%d %H:%M" "$to" "+%s")


query="SELECT cpu, CAST(ROUND(AVG(Load),0) as INT) FROM data WHERE ip = \"$1\" AND timestamp BETWEEN $startTs AND $endTs GROUP BY cpu;"

result=(`echo -e $query | sqlite3 -batch $db`)

for line in ${result[*]}
do
  data=(${line//|/ })
  printf "%s: %s%%\n" ${data[0]} ${data[1]}
done
