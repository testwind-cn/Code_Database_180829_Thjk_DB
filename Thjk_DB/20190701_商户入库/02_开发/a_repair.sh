#!/bin/bash

# 30个 ods_ftp.backup_dwd_merchant_info_yyyyMMdd 表，之前没有厂家分区，增加厂家分区 corp_id=10001

thedir="$(dirname $0)/"

cd ${thedir}


# 对当前表，加一个厂家分区
sudo -u hdfs hive -e 'ALTER TABLE dw_2g.dwd_merchant_info  RENAME TO dw_2g.dwd_merchant_info_2;'
# sudo -u admin hive -e 'CREATE TABLE dw_2g.dwd_merchant_info .... PARTITIONED BY ( corp_id) ...'
sudo -u hdfs hadoop dfs -mv /user/hive/warehouse/dw_2g.db/dwd_merchant_info_2  /user/hive/warehouse/dw_2g.db/dwd_merchant_info/corp_id=10001
sudo -u hdfs hive -e 'MSCK REPAIR TABLE dw_2g.dwd_merchant_info; drop TABLE if exists dw_2g.dwd_merchant_info_2;'


# 对历史表，原来有日期分区，在上层再加一个厂家分区
sudo -u hdfs hive -e 'ALTER TABLE dw_2g.dwd_merchant_info_history  RENAME TO dw_2g.dwd_merchant_info_history_2;'
# sudo -u admin hive -e 'CREATE TABLE dw_2g.dwd_merchant_info_history .... PARTITIONED BY ( corp_id, act_end ) ...'
sudo -u hdfs hadoop dfs -mv /user/hive/warehouse/dw_2g.db/dwd_merchant_info_history_2  /user/hive/warehouse/dw_2g.db/dwd_merchant_info_history/corp_id=10001
sudo -u hdfs hive -e 'MSCK REPAIR TABLE dw_2g.dwd_merchant_info_history; drop TABLE if exists dw_2g.dwd_merchant_info_history_2;'





DATE_S=$(date -d "20190707 -1 day" "+%Y%m%d")

for ((i=0;DATE_S<$(date -d "20190805 -1 day" "+%Y%m%d");i++))  # 处理到 20190805 那天，修复文件夹20190707-20190804
do
    DATE_S=$(date -d "20190707 +${i} day" "+%Y%m%d")
    echo "参数调用："${DATE_S} ${i}
    sudo -u hdfs hive -e "ALTER TABLE ods_ftp.backup_dwd_merchant_info_${DATE_S} RENAME TO ods_ftp.backup_dwd_merchant_info_${DATE_S}_2;"
    sudo -u admin hive --hivevar THE_DATE=${DATE_S} -f a_repair.sql
    sudo -u hdfs hadoop dfs -mv /user/hive/warehouse/ods_ftp.db/backup_dwd_merchant_info_${DATE_S}_2  /user/hive/warehouse/ods_ftp.db/backup_dwd_merchant_info_${DATE_S}/corp_id=10001
    sudo -u hdfs hive -e "MSCK REPAIR TABLE ods_ftp.backup_dwd_merchant_info_${DATE_S}; drop TABLE if exists ods_ftp.backup_dwd_merchant_info_${DATE_S}_2 ;"
done








