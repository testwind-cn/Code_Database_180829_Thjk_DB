#!/bin/bash
# 在100上运行 main.sh

thedir="$(dirname $0)/"
cd ${thedir}


FILE_NAME="Mcht_APMS_2nd_"
SOURCE_DIR="/ftpdata/thblposloan/merchantsinfo/"
LOCAL_DIR="/home/data/allinpay_data/merchantsinfo/"
OPT_DIR="/data/ods_ftp/opt/"

if [[ $# -ge 1 ]]
then
# "参数大于等于1"
    DATE_L=$1
else
    DATE_L=$(date -d "-1 day" "+%Y%m%d")
fi

echo "当前处理日期为："${DATE_L}

ssh 10.91.1.19 "lftp -u root,Redhat@2016 sftp://172.31.130.14:22 <<EOF
get ${SOURCE_DIR}${DATE_L}/${FILE_NAME}${DATE_L}.txt -o ${LOCAL_DIR}${FILE_NAME}${DATE_L}.txt
close
bye
EOF"

sftp 10.91.1.19<<EOF
get -P ${LOCAL_DIR}${FILE_NAME}${DATE_L}.txt ${OPT_DIR}${FILE_NAME}${DATE_L}.txt
EOF

iconv -f GB18030 -t UTF-8 ${OPT_DIR}${FILE_NAME}${DATE_L}.txt | sed -e '1d' -e 's/\\"/"/g' > ${OPT_DIR}${FILE_NAME}${DATE_L}_utf_mdfy.del

#   su admin -c "hive -e \"load data local inpath '${OPT_DIR}${FILE_NAME}${DATE_L}_utf_mdfy.del'  into table ods_ftp.merchant_update partition ( p_date = ${DATE_L} );\""
sudo -u admin hive -e "load data local inpath '${OPT_DIR}${FILE_NAME}${DATE_L}_utf_mdfy.del'  into table ods_ftp.merchant_update partition ( p_date = ${DATE_L} );"

rm -f ${OPT_DIR}${FILE_NAME}${DATE_L}.txt
rm -f ${OPT_DIR}${FILE_NAME}${DATE_L}_utf_mdfy.del

echo "开始处理日期" ${DATE_L} "***********************"

# 上传到 Hive

# ${thedir}drop.sh dw_2g.tmp_dwd_merchant_info

DATE_BK=$(date -d "${DATE_L} -20 day" "+%Y%m%d")

sudo -u admin hive -e "$(sed 's/${THE_DATE}/'${DATE_L}'/g' main_sub_01.sql)"

# ${thedir}drop.sh dw_2g.backup_dwd_merchant_info_${DATE_BK}

sudo -u admin hive -e "$(sed -e 's/${BACKUP_DATE}/'${DATE_BK}'/g' -e 's/${THE_DATE}/'${DATE_L}'/g' main_sub_02.sql)"



