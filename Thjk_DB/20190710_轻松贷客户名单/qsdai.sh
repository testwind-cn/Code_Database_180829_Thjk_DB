#!/bin/bash
# 在100上运行

su admin -c "hive -f /data/ods_ftp/qsdai.sql"

ssh 10.91.1.19 "/app/code/venv/venv1_hive/bin/python3  /app/code/qsdai/qsdai.py"

THE_DATE=$(date "+%Y%m%d")

ssh 10.91.1.19 "mv /home/data/qsdai/qsdai_${THE_DATE}.xlsx  /home/thjk01/thzc/qsdai/qsdai_${THE_DATE}"
