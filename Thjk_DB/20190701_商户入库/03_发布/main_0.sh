#!/bin/bash
# 在100上运行 main_dml_00.sql  初始化数据库

thedir="$(dirname $0)/"
THE_DATE=$(date "+%Y%m%d")

cd ${thedir}

su admin -c "hive -f main_dml_00.sql"

i=0
DATE_S=$(date -d "20180502 +${i} day" "+%Y%m%d")

for ((i=0;DATE_S<$(date -d "-1 day" "+%Y%m%d");i++))
do
    DATE_S=$(date -d "20180502 +${i} day" "+%Y%m%d")
    echo "参数调用："${DATE_S} ${i}
    sh ./main.sh ${DATE_S}
done