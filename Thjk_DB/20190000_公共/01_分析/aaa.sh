#!/bin/sh
if [ $# -eq 0 ]
then
i=-1
else
i=$1
fi
DATE_Y_M=$(date -d "${i} day" +%Y%m)
DATE_D=$(date -d "${i} day" +%d)
DATE=$(date -d "${i} day" +%Y%m%d)
DATAWORKPATH=/home/thbldb01/data/output

find ${DATAWORKPATH}/*/${DATE_Y_M}${DATE_D}/ -name '*.csv' -exec sh -c 'echo "$1";iconv -f utf-8//IGNORE -t gbk//IGNORE "$1" -o "${1%.*}_gbk.data"' _ {} \;
mkdir ${DATAWORKPATH}/toaif/${DATE_Y_M}${DATE_D}
mv ${DATAWORKPATH}/syb_posflow_increment/${DATE_Y_M}${DATE_D}/*gbk.data  ${DATAWORKPATH}/toaif/${DATE_Y_M}${DATE_D}/${DATE_Y_M}${DATE_D}_shouyinbao_rsp_bl.del
mv ${DATAWORKPATH}/syb_mer_increment/${DATE_Y_M}${DATE_D}/*gbk.data ${DATAWORKPATH}/toaif/${DATE_Y_M}${DATE_D}/Mcht_SYB_2nd_${DATE_Y_M}${DATE_D}.txt
mv ${DATAWORKPATH}/syb_acct_increment/${DATE_Y_M}${DATE_D}/*gbk.data ${DATAWORKPATH}/toaif/${DATE_Y_M}${DATE_D}/Acct_SYB_2nd_${DATE_Y_M}${DATE_D}.txt

~


iconv -f utf-8//IGNORE -t gbk//IGNORE "$1" -o "${1%.*}_gbk.data