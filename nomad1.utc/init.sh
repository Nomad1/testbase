#!/bin/bash

db="test.db"

path=$1
if [ -z "$path" ]; then
    echo "PATH is empty"
    exit
fi

files=($path/*)

query="drop table if exists data;\ncreate table data(Timestamp int, IP text, CPU int, Load int);\ncreate index ipcpu_index on data(ip, cpu);\n.separator ' '\n";


for item in ${files[*]}
do
  query="$query.import $item data\n"
done

query="$query\nselect count(*) from data"

echo -e $query | sqlite3 -batch $db
