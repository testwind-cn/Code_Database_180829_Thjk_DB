


dim.dwd_merchant_info_bk
dim.dwd_merchant_info_history_bk
    20190708处理，
    只有821没有收银宝，
    增量文件范围 20180502-20190704，
    按结束日期分区了，没分桶
    CSV格式



ods_ftp.backup_dwd_merchant_info_20190706
ods_ftp.backup_dwd_merchant_info_20190804
dim.dwd_merchant_info
dim.dwd_merchant_info_history
    20190805处理，
    有821和收银宝，但在一个文件夹 corp_id=10001
    增量文件范围 20180423-20190804，
    不按日期分区，分桶20个
    SEQ格式




dim.bk_dim_merchant_info
dim.bk_dim_merchant_info_history
ods_ftp.bk_backup_dim_merchant_info_20190701
    20190808-19:19
    用 20190122 的初始化，再用 20190701 的更新，用来分析差异
    分析文件在：
        190808_分析增量新增情况.sql
        190808_分析增量覆盖情况.sql


dim.dim_manager_code
    每天晚上11点跑批更新的客户经理表

dim.dim_merchant
dim.tmp_dim_merchant
    手工做的简化商户维度表
    文本格式



ods_ftp_opt.dim_identity_manager
ods_ftp_opt.tmp_dim_identity_manager
    开发客户经理身份证