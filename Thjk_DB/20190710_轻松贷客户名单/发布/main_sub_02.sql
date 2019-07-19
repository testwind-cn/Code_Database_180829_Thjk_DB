
drop table if exists `ods_ftp`.`qsd_merchant`;

ALTER TABLE `ods_ftp`.`qsd_merchant_temp` RENAME TO `ods_ftp`.`qsd_merchant`;
