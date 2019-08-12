#!/bin/bash
# 在100上运行，先运行 main_01.sh 下载和入库增量文件，再运行 main_02.sh 执行  Hive SQL 处理逻辑
# 在100上运行 main.sh，实现每日增量调整入库 merchant_update，并更新 dwd_merchant_info、dwd_merchant_info_history 表。历史表备份到 backup_dwd_merchant_info_${THE_DATE};

thedir="$(dirname $0)/"
cd ${thedir}
# 切换文件夹到当前执行文件的路径

FILE_NAME1="Mcht_APMS_2nd_"
FILE_NAME2="Mcht_SYB_2nd_"
SOURCE_DIR1="/ftpdata/thblposloan/merchantsinfo/"
SOURCE_DIR2="/home/thbldb01/data/output/toaif/"
LOCAL_DIR1="/home/data/allinpay_data/merchantsinfo/"
LOCAL_DIR2="/home/data/SYB_DATA/merchantsinfo/"
OPT_DIR="/data/ods_ftp/opt/"


if [[ $# -ge 1 ]]
then
# 命令行参数大于等于1，则使用参数作为日期，否则取当前日期前一天
    DATE_L=$1
else
    DATE_L=$(date -d "-1 day" "+%Y%m%d")
fi


echo "开始下载上传处理，日期：" ${DATE_L} "***********************"


# SSH远程到19服务器，让19执行 lftp 命令，用<<传递FTP指令
ssh 10.91.1.19 "lftp -u root,Redhat@2016 sftp://172.31.130.14:22 <<EOF
get ${SOURCE_DIR1}${DATE_L}/${FILE_NAME1}${DATE_L}.txt -o ${LOCAL_DIR1}${FILE_NAME1}${DATE_L}.txt
close
bye
EOF"


# SSH远程到19服务器，让19执行 lftp 命令，用<<传递FTP指令 /home/thbldb01/data/output/toaif/20190802/Mcht_SYB_2nd_20190802.txt
ssh 10.91.1.19 "lftp -u root,Redhat@2016 sftp://172.31.130.16:22 <<EOF
get ${SOURCE_DIR2}${DATE_L}/${FILE_NAME2}${DATE_L}.txt -o ${LOCAL_DIR2}${FILE_NAME2}${DATE_L}.txt
close
bye
EOF"


# SFTP到19服务器，下载文件到本地
sftp 10.91.1.19<<EOF
get -P ${LOCAL_DIR1}${FILE_NAME1}${DATE_L}.txt ${OPT_DIR}${FILE_NAME1}${DATE_L}.txt
get -P ${LOCAL_DIR2}${FILE_NAME2}${DATE_L}.txt ${OPT_DIR}${FILE_NAME2}${DATE_L}.txt
EOF


# 将下载的文件进行转码，并流式删除掉第一行，再把 \" 替换成 " ，保存文件到本地
iconv -f GB18030 -t UTF-8 ${OPT_DIR}${FILE_NAME1}${DATE_L}.txt | sed -e '1d' -e 's/\\"/"/g' > ${OPT_DIR}${FILE_NAME1}${DATE_L}_utf_mdfy.del
iconv -f GB18030 -t UTF-8 ${OPT_DIR}${FILE_NAME2}${DATE_L}.txt | sed -e '1d' -e 's/\\"/"/g' > ${OPT_DIR}${FILE_NAME2}${DATE_L}_utf_mdfy.del


# 使用 admin 账号执行 Hive 命令，把本地文件加载到表中当前日期分区
#   su admin -c "hive -e \"load data local inpath '${OPT_DIR}${FILE_NAME1}${DATE_L}_utf_mdfy.del'  into table ods_ftp.merchant_update partition ( p_date = ${DATE_L} );\""
sudo -u admin hive -e "load data local inpath '${OPT_DIR}${FILE_NAME1}${DATE_L}_utf_mdfy.del'  into table ods_ftp.merchant_update partition ( p_date = ${DATE_L} );"
sudo -u admin hive -e "load data local inpath '${OPT_DIR}${FILE_NAME2}${DATE_L}_utf_mdfy.del'  into table ods_ftp.merchant_update partition ( p_date = ${DATE_L} );"

# 删除临时文件
rm -f ${OPT_DIR}${FILE_NAME1}${DATE_L}.txt
rm -f ${OPT_DIR}${FILE_NAME2}${DATE_L}.txt
rm -f ${OPT_DIR}${FILE_NAME1}${DATE_L}_utf_mdfy.del
rm -f ${OPT_DIR}${FILE_NAME2}${DATE_L}_utf_mdfy.del


echo "完成下载上传处理，日期：" ${DATE_L} "***********************"



