#!/bin/bash
# 在100上运行，先运行 main_01.sh 下载和入库增量文件，再运行 main_02.sh 执行  Hive SQL 处理逻辑
# 在100上运行 main.sh，实现每日增量调整入库 merchant_update，并更新 dwd_merchant_info、dwd_merchant_info_history 表。历史表备份到 backup_dwd_merchant_info_${THE_DATE};

thedir="$(dirname $0)/"
cd ${thedir}
# 切换文件夹到当前执行文件的路径


if [[ $# -ge 1 ]]
then
# 命令行参数大于等于1，则使用参数作为日期，否则取当前日期前一天
    DATE_L=$1
else
    DATE_L=$(date -d "-1 day" "+%Y%m%d")
fi


# Hive SQL 处理

echo "开始处理SQL逻辑：日期：" ${DATE_L} "***********************"

# sh ${thedir}drop.sh dim.tmp_dim_merchant_info

# 把 SQL 文件中的日期替换成当天，用 admin 账号执行 SQL 命令
# sudo -u admin hive -e "$(sed 's/${THE_DATE}/'${DATE_L}'/g' main_dml_01.sql)"
# sudo -u admin hive -e "SET hivevar:THE_DATE=${DATE_L}; $(cat main_dml_01.sql)"
sudo -u admin hive --hivevar THE_TABLE=dim.tmp_dim_merchant_info --hivevar THE_DATE=${DATE_L} -e "$(cat main_ddl_01.sql ; cat main_dml_01.sql)"

# 每天新生存全表后，老表改名备份，删除30天前的备份表
DATE_BK=$(date -d "${DATE_L} -30 day" "+%Y%m%d")

# sh ${thedir}drop.sh dim.backup_dim_merchant_info_${DATE_BK}

# 把 SQL 文件中的日期替换成当天，备份日期替换成30天前，用 admin 账号执行 SQL 命令
# sudo -u admin hive -e "$(sed -e 's/${BACKUP_DATE}/'${DATE_BK}'/g' -e 's/${THE_DATE}/'${DATE_L}'/g' main_dm_02.sql)"
sudo -u hdfs hive --hivevar THE_DATE=${DATE_L} --hivevar BACKUP_DATE=${DATE_BK} -f main_dml_02.sql

echo "完成处理SQL逻辑：日期：" ${DATE_L} "***********************"

