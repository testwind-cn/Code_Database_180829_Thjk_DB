-- 正则替换掉注释      (?:^|[ ]+)(?:#|--) +[^\n]*

-- use 语句 和  insert , select 语句尾部不能加分号。 set 语句可以加分号

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