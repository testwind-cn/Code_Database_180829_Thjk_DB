#!/bin/bash


THEDIR="$(dirname $0)/"
THE_DATE=$(date "+%Y%m%d")

cd ${THEDIR}

i=-1
DATE_S=$(date -d "${i} day" "+%Y%m%d")

sh ./main_01.sh ${DATE_S}

sh ./main_02.sh ${DATE_S}
