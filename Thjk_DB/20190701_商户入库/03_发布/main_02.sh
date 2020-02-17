#!/bin/bash



THEDIR="$(dirname $0)/"
cd ${THEDIR}

TABLE_S=dim.tmp_dim_merchant_info

if [[ $# -ge 1 ]]
then

    DATE_L=$1
else
    DATE_L=$(date -d "-1 day" "+%Y%m%d")
fi




echo "开始处理SQL逻辑：日期：" ${DATE_L} "***********************"






sudo -u admin hive --hivevar THE_TABLE=${TABLE_S} --hivevar THE_DATE=${DATE_L} -e "$(cat main_ddl_01.sql ; cat main_dml_01.sql)"


DATE_BK=$(date -d "${DATE_L} -30 day" "+%Y%m%d")





sudo -u hdfs hive --hivevar THE_DATE=${DATE_L} --hivevar BACKUP_DATE=${DATE_BK} -f main_dml_02.sql

echo "完成处理SQL逻辑：日期：" ${DATE_L} "***********************"

