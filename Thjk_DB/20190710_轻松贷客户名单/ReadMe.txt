sftp://root@10.91.1.19/app/code/qsdai/
    qsdai.py
    conf.py

sftp://root@10.91.1.100/data/ods_ftp/
    qsdai.sh
    qsdai.sql

qsdai.job
    type=command
    command=ssh 10.91.1.100 "/data/ods_ftp/qsdai.sh"
