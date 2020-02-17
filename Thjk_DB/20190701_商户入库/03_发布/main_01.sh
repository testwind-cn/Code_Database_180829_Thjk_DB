#!/bin/bash



THEDIR="$(dirname $0)/"
cd ${THEDIR}


FILE_NAME1="Mcht_APMS_2nd_"
FILE_NAME2="Mcht_SYB_2nd_"
SOURCE_DIR1="/ftpdata/thblposloan/merchantsinfo/"
SOURCE_DIR2="/home/thbldb01/data/output/toaif/"
LOCAL_DIR1="/home/data/allinpay_data/merchantsinfo/"
LOCAL_DIR2="/home/data/SYB_DATA/merchantsinfo/"
OPT_DIR="/data/ods_ftp/opt/"


if [[ $# -ge 1 ]]
then

    DATE_L=$1
else
    DATE_L=$(date -d "-1 day" "+%Y%m%d")
fi


echo "开始下载上传处理，日期：" ${DATE_L} "***********************"



ssh 10.91.1.19 "lftp -u root,Redhat@2016 sftp://172.31.130.14:22 <<EOF
get ${SOURCE_DIR1}${DATE_L}/${FILE_NAME1}${DATE_L}.txt -o ${LOCAL_DIR1}${FILE_NAME1}${DATE_L}.txt
close
bye
EOF"



ssh 10.91.1.19 "lftp -u root,Redhat@2016 sftp://172.31.130.16:22 <<EOF
get ${SOURCE_DIR2}${DATE_L}/${FILE_NAME2}${DATE_L}.txt -o ${LOCAL_DIR2}${FILE_NAME2}${DATE_L}.txt
close
bye
EOF"



sftp 10.91.1.19<<EOF
get -P ${LOCAL_DIR1}${FILE_NAME1}${DATE_L}.txt ${OPT_DIR}${FILE_NAME1}${DATE_L}.txt
get -P ${LOCAL_DIR2}${FILE_NAME2}${DATE_L}.txt ${OPT_DIR}${FILE_NAME2}${DATE_L}.txt
EOF



iconv -f GB18030 -t UTF-8 ${OPT_DIR}${FILE_NAME1}${DATE_L}.txt | sed -e '1d' -e 's/\\"/"/g' > ${OPT_DIR}${FILE_NAME1}${DATE_L}_utf_mdfy.del
iconv -f GB18030 -t UTF-8 ${OPT_DIR}${FILE_NAME2}${DATE_L}.txt | sed -e '1d' -e 's/\\"/"/g' > ${OPT_DIR}${FILE_NAME2}${DATE_L}_utf_mdfy.del




sudo -u admin hive -e "load data local inpath '${OPT_DIR}${FILE_NAME1}${DATE_L}_utf_mdfy.del'  into table ods_ftp.merchant_update partition ( p_date = ${DATE_L} );"
sudo -u admin hive -e "load data local inpath '${OPT_DIR}${FILE_NAME2}${DATE_L}_utf_mdfy.del'  into table ods_ftp.merchant_update partition ( p_date = ${DATE_L} );"


rm -f ${OPT_DIR}${FILE_NAME1}${DATE_L}.txt
rm -f ${OPT_DIR}${FILE_NAME2}${DATE_L}.txt
rm -f ${OPT_DIR}${FILE_NAME1}${DATE_L}_utf_mdfy.del
rm -f ${OPT_DIR}${FILE_NAME2}${DATE_L}_utf_mdfy.del


echo "完成下载上传处理，日期：" ${DATE_L} "***********************"



