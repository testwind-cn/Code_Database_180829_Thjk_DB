#!/bin/bash
# 在100上运行 opt.sh
i=0
DATE_L=$(date -d "20180502 +${i} day" "+%Y%m%d")

for ((i=0;DATE_L<$(date -d "-1 day" "+%Y%m%d");i++))
do
    DATE_L=$(date -d "20180502 +${i} day" "+%Y%m%d")
    echo ${DATE_L} ${i}

#    sftp 10.91.1.19<<EOF
#get /home/data/allinpay_data/merchantsinfo/Mcht_APMS_2nd_${DATE_L}.txt /data/ods_ftp/opt/Mcht_APMS_2nd_${DATE_L}.txt
#EOF
    iconv -f GB18030 -t UTF-8 /data/ods_ftp/opt/Mcht_APMS_2nd_${DATE_L}.txt | sed -e '1d' -e 's/\\"/"/g' > /data/ods_ftp/opt/Mcht_APMS_2nd_${DATE_L}_utf_mdfy.del
    hive -e "load data local inpath '/data/ods_ftp/opt/Mcht_APMS_2nd_${DATE_L}_utf_mdfy.del'  into table ods_ftp.merchant_update partition ( p_date = ${DATE_L} );"

done

