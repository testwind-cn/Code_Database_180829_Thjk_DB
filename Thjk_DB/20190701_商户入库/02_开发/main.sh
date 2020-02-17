#!/bin/bash
# 在100上运行，先运行 main_01.sh 下载和入库增量文件，再运行 main_02.sh 执行  Hive SQL 处理逻辑

THEDIR="$(dirname $0)/"
THE_DATE=$(date "+%Y%m%d")

cd ${THEDIR}

i=-1
DATE_S=$(date -d "${i} day" "+%Y%m%d")

sh ./main_01.sh ${DATE_S}

sh ./main_02.sh ${DATE_S}
