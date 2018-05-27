#!/bin/bash

path=$1
if [ -z "$path" ]; then
    echo "PATH is empty"
    exit
fi

from="2014-10-31 00:00"
to="2014-10-31 23:59"
offset=60
ips=1000
cpus=2

startTs=$(date -j -f "%Y-%m-%d %H:%M" "$from" "+%s")
endTs=$(date -j -f "%Y-%m-%d %H:%M" "$to" "+%s")

count=$(((($endTs-$startTs)/$offset + 1)*ips*cpus));
echo "Generating $count records and $ips files"

rnd=($(dd if=/dev/urandom bs=1 count=$count 2> /dev/null | od -An -tu1))

i=0
ip=0

while [ "$ip" -lt "$ips" ]
do 
  ipstr="$((($ip/16516350) + 10)).$((($ip/64770) % 255)).$((($ip/254) % 255)).$((($ip % 254) + 1))"
  echo "Working with $ipstr"
  rm $path/$ipstr.log 2> /dev/null
  ts=$startTs
  while [ "$ts" -lt "$endTs" ]
  do
    cpu=0
    while [ "$cpu" -lt "$cpus" ]
    do
      load=$(((${rnd[$i]})%101))
      printf "$ts $ipstr $cpu $load\n">> $path/$ipstr.log
      ((cpu+=1))
      ((i+=1))
    done
    ((ts+=$offset))
  done
  ((ip+=1))
done
