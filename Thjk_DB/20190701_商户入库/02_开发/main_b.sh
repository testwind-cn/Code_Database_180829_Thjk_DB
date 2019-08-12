#!/bin/bash
# 在100上运行 main_dml_00.sql  初始化数据库，循环执行 main.sh ，从20180502 到当前日期

thedir="$(dirname $0)/"
THE_DATE=$(date "+%Y%m%d")

cd ${thedir}


sudo -u admin hive --hivevar THE_TABLE=dim.dim_merchant_info -e "$(cat main_ddl_00.sql ; cat main_ddl_01.sql ; cat main_dml_00.sql)"


DATE_S=$(date -d "20180423 -1 day" "+%Y%m%d")

for ((i=0;DATE_S<$(date -d "20190801 -1 day" "+%Y%m%d");i++))  # 执行到  20190801 当天， 处理 20180423-20190731 的文件
# for ((i=0;DATE_S<$(date -d "-1 day" "+%Y%m%d");i++))           # 执行到 今天， 处理 20180423-昨天日期 的文件
do
    DATE_S=$(date -d "20180423 +${i} day" "+%Y%m%d")
    echo "参数调用："${DATE_S} ${i}
#    sh ./main_01.sh ${DATE_S}           # 执行文件下载
    sh ./main_02.sh ${DATE_S}           # 执行 SQL 逻辑
done