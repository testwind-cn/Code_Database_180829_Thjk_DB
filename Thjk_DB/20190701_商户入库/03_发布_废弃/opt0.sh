#!/bin/bash
# 在19-FTP上运行

i=0
DATE_L=$(date -d "20180502 +${i} day" "+%Y%m%d")
# DATE_C=$(date "+%Y%m%d")
for ((i=0;DATE_L<$(date -d "-1 day" "+%Y%m%d");i++))
do
    DATE_L=$(date -d "20180502 +${i} day" "+%Y%m%d")
    echo ${DATE_L} ${i}

    lftp -u root,Redhat@2016 sftp://172.31.130.14:22 <<EOF
get /ftpdata/thblposloan/merchantsinfo/${DATE_L}/Mcht_APMS_2nd_${DATE_L}.txt -o /home/data/allinpay_data/merchantsinfo/Mcht_APMS_2nd_${DATE_L}.txt
close
bye
EOF

done
