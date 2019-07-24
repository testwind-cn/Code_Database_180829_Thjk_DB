
drop table if exists `dm_2g`.`qsd_merchant`;

ALTER TABLE `dm_2g`.`tmp_qsd_merchant` RENAME TO `ods_ftp`.`qsd_merchant`;
