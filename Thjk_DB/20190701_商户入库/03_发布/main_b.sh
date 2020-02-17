#!/bin/bash


THEDIR="$(dirname $0)/"
THE_DATE=$(date "+%Y%m%d")
TABLE_S=dim.dim_merchant_info

cd ${THEDIR}





DATE_S=$(date -d "20180423 -1 day" "+%Y%m%d")

for ((i=0;DATE_S<$(date -d "20190801 -1 day" "+%Y%m%d");i++))

do
    DATE_S=$(date -d "20180423 +${i} day" "+%Y%m%d")
    echo "参数调用："${DATE_S} ${i}

    sh ./main_02.sh ${DATE_S}
done