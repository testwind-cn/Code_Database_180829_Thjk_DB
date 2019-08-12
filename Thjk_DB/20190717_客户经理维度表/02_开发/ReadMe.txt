-- 正则替换掉注释      (?:^|[ ]+)(?:#|--) +[^\n]*

sftp://root@10.91.1.100/submit/shells/dim_code_manager/
    drop.sh
    main.sh
    main_sub_01.sql
    main_sub_02.sql
    MaxStrUDAF.jar

job
    dim_code_manager.job
    type=command
    command=ssh 10.91.1.100 "/submit/shells/dim_code_manager/main.sh"


/*
sftp://root@10.91.1.19/app/code/dim_code_manager/
*/