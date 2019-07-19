-- 正则替换掉注释      [ ]*--[^\n]*

sftp://root@10.91.1.100/submit/shells/qsdai/
    drop.sh
    main.sh
    main_sub_01.sql
    main_sub_02.sql

job
    qsdai.job
    type=command
    command=ssh 10.91.1.100 "/submit/shells/qsdai/main.sh"


sftp://root@10.91.1.19/app/code/qsdai/
    conf.py
    qsdai.ini
    qsdai.py
    qsdai.sql