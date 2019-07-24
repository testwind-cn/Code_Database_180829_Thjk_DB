
drop table if exists `dm_2g`.`dm_qsd_merchant`;

ALTER TABLE `dm_2g`.`tmp_dm_qsd_merchant` RENAME TO `ods_ftp`.`dm_qsd_merchant`;
