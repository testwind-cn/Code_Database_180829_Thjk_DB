#!/bin/bash

sudo -u hdfs hive -e "show create table $1;"  | if ((1==1))
then
    del_sh=""
    while read aline
    do
        del_sh="${del_sh}\n${aline}"
    done


    sudo -u hdfs hive -e "drop table if exists $1;"

    the_table=$(echo -e "${del_sh}" | sed -n  -e '/^.*8020\//p' | sed -e 's/^.*8020\//\//g' -e 's/'\''//g')

    echo "PATH：" ${the_table}

    for atable in ${the_table}
    do
        sudo -u hdfs hdfs dfs -rm -f -r -skipTrash ${atable}
        echo "OK" ${atable}
    done
fi