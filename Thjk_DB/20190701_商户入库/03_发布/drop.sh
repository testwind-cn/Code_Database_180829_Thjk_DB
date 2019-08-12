#!/bin/bash

sudo -u hdfs hive -e "show create table $1;"  | if ((1==1))
then
    DEL_SH=""
    while read ALINE
    do
        DEL_SH="${DEL_SH}\n${ALINE}"
    done

    sudo -u hdfs hive -e "drop table if exists $1;"

    the_table=$(echo -e "${DEL_SH}" | sed -n  -e '/^.*8020\//p' | sed -e 's/^.*8020\//\//g' -e 's/'\''//g')

    echo "PATH：" ${THE_TABLE}

    for ATABLE in ${the_table}
    do
        sudo -u hdfs hdfs dfs -rm -f -r -skipTrash ${ATABLE}
        echo "OK" ${ATABLE}
    done
fi