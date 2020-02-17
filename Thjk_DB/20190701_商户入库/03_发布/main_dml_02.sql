



DROP TABLE IF EXISTS ods_ftp.backup_dim_merchant_info_${hivevar:BACKUP_DATE};

ALTER TABLE dim.dim_merchant_info RENAME TO ods_ftp.backup_dim_merchant_info_${hivevar:THE_DATE};

ALTER TABLE dim.tmp_dim_merchant_info RENAME TO dim.dim_merchant_info;
