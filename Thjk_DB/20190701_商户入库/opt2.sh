#!/bin/bash
# 在100上运行 opt2.sh  处理 20180502 - 20190704 的文件

hive -f merchant_opt1.sql

i=0
DATE_L=$(date -d "20180502 +${i} day" "+%Y%m%d")

for ((i=0;DATE_L<$(date -d "20190705 -1 day" "+%Y%m%d");i++))
do
    DATE_L=$(date -d "20180502 +${i} day" "+%Y%m%d")
    echo "处理日期" ${DATE_L} ${i} "***********************"

# 上传到 Hive
    hive -e "$(sed 's/${THE_DATE}/'${DATE_L}'/g' merchant_opt2.sql)"

done
