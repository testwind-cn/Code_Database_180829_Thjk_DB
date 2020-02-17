#!/bin/bash
# 使用 hdfs 账号，执行 Hive 命令，以参数中的表名，来查询表信息。需要一个if语句来嵌套，保证|流式传递后面是一个整语句。整语句中的变量始终有效
sudo -u hdfs hive -e "show create table $1;"  | if ((1==1))
then
    DEL_SH=""
# 从流式输入中，按行读取输入到 ALINE 变量
    while read ALINE
    do
        DEL_SH="${DEL_SH}\n${ALINE}"
    done

    sudo -u hdfs hive -e "drop table if exists $1;"
# 使用 hdfs 账号，执行 Hive 命令，删除 Hive 中的表数据
    THE_TABLE=$(echo -e "${DEL_SH}" | sed -n  -e '/^.*8020\//p' | sed -e 's/^.*8020\//\//g' -e 's/'\''//g')
# 从表信息输出文本中，1、查询包含 hdfs://xxxx:8020/ 的行，2、替换成没有，3、替换掉'。后面的文本就是表文件的位置
# LOCATION
#  'hdfs://nn1:8020/user/hive/warehouse/rds_posflow.db/base_date'
    echo "PATH：" ${THE_TABLE}
# 对每个文件位置，使用 hdfs 账号执行 Hadoop 命令，删除对应文件夹
    for ATABLE in ${THE_TABLE}
    do
        sudo -u hdfs hdfs dfs -rm -f -r -skipTrash ${ATABLE}
        echo "OK" ${ATABLE}
    done
fi