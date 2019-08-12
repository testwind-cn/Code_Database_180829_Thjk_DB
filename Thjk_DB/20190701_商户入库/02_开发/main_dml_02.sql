-- 1、将30天前的备份表删除 backup_dim_merchant_info_${BACKUP_DATE}
-- 2、将当前的 dwd_merchant_info 表改名成 backup_dim_merchant_info_${THE_DATE}
-- 3、将 tmp_dim_merchant_info 表改名成 dim_merchant_info

DROP TABLE IF EXISTS ods_ftp.backup_dim_merchant_info_${hivevar:BACKUP_DATE};

ALTER TABLE dim.dim_merchant_info RENAME TO ods_ftp.backup_dim_merchant_info_${hivevar:THE_DATE};

ALTER TABLE dim.tmp_dim_merchant_info RENAME TO dim.dim_merchant_info;
