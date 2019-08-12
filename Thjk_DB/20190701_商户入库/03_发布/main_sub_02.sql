DROP TABLE IF EXISTS dw_2g.backup_dwd_merchant_info_${BACKUP_DATE};

ALTER TABLE dw_2g.dwd_merchant_info RENAME TO dw_2g.backup_dwd_merchant_info_${THE_DATE};

ALTER TABLE dw_2g.tmp_dwd_merchant_info RENAME TO dw_2g.dwd_merchant_info;
