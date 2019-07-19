#!/bin/bash
# 在100上运行

thedir="$(dirname $0)/"

cd ${thedir}

${thedir}drop.sh ods_ftp.qsd_merchant_temp

sudo -u admin hive -f ${thedir}main_sub_01.sql

${thedir}drop.sh ods_ftp_opt.qsd_merchant

sudo -u admin hive -f  ${thedir}main_sub_02.sql

ssh 10.91.1.19 "/app/code/venv/venv1_hive/bin/python3  /app/code/qsdai/qsdai.py"

THE_DATE=$(date "+%Y%m%d")

ssh 10.91.1.19 "mv /home/data/qsdai/qsdai_${THE_DATE}.xlsx  /home/thjk01/thzc/qsdai/qsdai_${THE_DATE}.xlsx"
