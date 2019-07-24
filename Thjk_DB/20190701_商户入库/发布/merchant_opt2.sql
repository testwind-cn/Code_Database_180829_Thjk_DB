

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 4000;
SET hive.exec.max.dynamic.partitions=4000;


-- ===设置map输出和reduce输出进行合并的相关参数：
-- #设置map端输出进行合并，默认为true
set hive.merge.mapfiles = true;

-- #设置reduce端输出进行合并，默认为false
set hive.merge.mapredfiles = true;

-- #设置合并文件的大小
set hive.merge.size.per.task = 256000000;

-- #当输出文件的平均大小小于该值时，启动一个独立的MapReduce任务进行文件merge。
set hive.merge.smallfiles.avgsize=16000000;


-- -------------------------------
drop table if exists `dw_2g`.`tmp_merchant_${THE_DATE}`;

CREATE TABLE `dw_2g`.`tmp_merchant_${THE_DATE}` (
  `dt_type` string COMMENT '记录类型', -- HIVE建表
  `mcht_cd`  string COMMENT '商户编码',
  `bran_cd`  string COMMENT '公司代码',
  `mcht_name`  string COMMENT '商户名称',  -- 通金导库文件里错写成：商户营业名称*
  `mcht_business_name`  string COMMENT '商户营业名称',  -- 通金导库文件里错写成：商户名称
  `mcht_org_cd`  string COMMENT '商户收单机构代码',
  `mcht_business_license`  string COMMENT '营业执照号码',
  `mcht_type`  string COMMENT '商户类型',
  `mcht_service_type`  string COMMENT '服务模式',
  `servcie_org`  string COMMENT '专业化服务机构',
  `reg_prov_cd`  string COMMENT '注册地址省代码',
  `reg_city_cd`  string COMMENT '注册地址地市代码',
  `reg_country_cd`  string COMMENT '注册地址区县代码',
  `mcht_address`  string COMMENT '商户营业地址',
  `contact_name`  string COMMENT '联系人',
  `contact_phone`  string COMMENT '联系人电话',
  `contact_fax`  string COMMENT '联系人传真',
  `contact_email`  string COMMENT '电子邮箱地址',
  `bloc_mcht_cd`  string COMMENT '集团商户代码',
  `bloc_mcht_name`  string COMMENT '集团商户名称',
  `mcht_settlement_way`  string COMMENT '商户结算途径',
  `approve_date`  string COMMENT '商户批准日期',
  `lose_date`  string COMMENT '商户失效日期',
  `service_level`  string COMMENT '服务等级',
  `source_from`  string COMMENT '商户来源渠道（发展方）',
  `referrer`  string COMMENT '发展人或推荐人',






  `customer_cd`  string COMMENT '统一客户代码',
  `customer_name`  string COMMENT '统一客户名称',
  `mcc_cd`  string COMMENT '国民经济行业分类代码',
  `org_cd`  string COMMENT '组织机构代码',
  `legal_person_name`  string COMMENT '法定代表人姓名',
  `legal_person_id_type`  string COMMENT '法定代表人证件类型',
  `legal_person_id_no`  string COMMENT '法定代表人证件号码',
  `mcht_settlement_cycle`  string COMMENT '商户结算周期',
  `normal_clt_count`  string COMMENT '正常终端数',
  `undo_clt_count`  string COMMENT '撤销终端数',
  `mcht_nature`  string COMMENT '商户性质',
  `mcht_status`  string COMMENT '商户状态',
  `is_risk_lp`  string COMMENT '法人代表是否为风险商户法人代表',
  `mcht_dept`  string COMMENT '商户负债额',
  `has_overdue`  string COMMENT '商户负责人是否有逾期债务',
  `financial_name`  string COMMENT '财务联系人',
  `financial_phone`  string COMMENT '财务联系人电话',

  `tax_reg_no`  string COMMENT '税务登记证号',

  `settlement_model`  string COMMENT '[结算模式*]',
  `settlement_cycle`  string COMMENT '[结算周期*]',
  `settlement_way`  string COMMENT '[结算途径*]',
  `data_channel`  string COMMENT '[数据渠道*]',
  `is_group_mcht`  string COMMENT '[是否为集团商户*]',
  `is_point_card_settlement`  string COMMENT '[是否分卡结算*]',
  `customer_no`  string COMMENT '[统一客户号*]',
  `area_standard`  string COMMENT '[地区标准*]',
  `reg_capital`  string COMMENT '[注册资本*]',
  `main_business`  string COMMENT '[主营业务*]',
  `scope_of_business`  string COMMENT '[经营范围*]',
  `business_license_limit`  string COMMENT '[营业执照有效期*]',
  `service_nature`  string COMMENT '[服务性质*]',
  `product_info`  string COMMENT '[条线产品信息*]',
  `mcht_mcc`  string COMMENT '[商户MCC(18)*]',

  `three_in_one_flag` string COMMENT '[         ]',
  `us_credit_code_no`  string COMMENT '[         ]',
  `us_credit_code_start_date`  string COMMENT '[         ]',
  `us_credit_code_end_date`  string COMMENT '[         ]',
  `act_end` int COMMENT '生效结束日期'
) PARTITIONED BY ( act_start INT COMMENT '生效开始日期分区')
 ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\"",
    "escapeChar" = "\\"
)
STORED AS TEXTFILE;


insert into table `dw_2g`.`tmp_merchant_${THE_DATE}`
    partition (act_start)
select
    `dt_type`,
    `mcht_cd`,
    `bran_cd`,
    `mcht_name`,
    `mcht_business_name`,
    `mcht_org_cd`,
    `mcht_business_license`,
    `mcht_type`,
    `mcht_service_type`,
    `servcie_org`,
    `reg_prov_cd`,
    `reg_city_cd`,
    `reg_country_cd`,
    `mcht_address`,
    `contact_name`,
    `contact_phone`,
    `contact_fax`,
    `contact_email`,
    `bloc_mcht_cd`,
    `bloc_mcht_name`,
    `mcht_settlement_way`,
    `approve_date`,
    `lose_date`,
    `service_level`,
    `source_from`,
    `referrer`,
    `customer_cd`,
    `customer_name`,
    `mcc_cd`,
    `org_cd`,
    `legal_person_name`,
    `legal_person_id_type`,
    `legal_person_id_no`,
    `mcht_settlement_cycle`,
    `normal_clt_count`,
    `undo_clt_count`,
    `mcht_nature`,
    `mcht_status`,
    `is_risk_lp`,
    `mcht_dept`,
    `has_overdue`,
    `financial_name`,
    `financial_phone`,
    `tax_reg_no`,
    `settlement_model`,
    `settlement_cycle`,
    `settlement_way`,
    `data_channel`,
    `is_group_mcht`,
    `is_point_card_settlement`,
    `customer_no`,
    `area_standard`,
    `reg_capital`,
    `main_business`,
    `scope_of_business`,
    `business_license_limit`,
    `service_nature`,
    `product_info`,
    `mcht_mcc`,
    `three_in_one_flag`,
    `us_credit_code_no`,
    `us_credit_code_start_date`,
    `us_credit_code_end_date`,
    `act_end`,
    `act_start`
from (
    SELECT dwd_merchant_info.*, aaa.mcht_cd as mcht_cd2
    from dw_2g.dwd_merchant_info
    LEFT JOIN (
        select mcht_cd from ods_ftp.merchant_update where p_date=${THE_DATE}
    ) aaa
    on dwd_merchant_info.mcht_cd = aaa.mcht_cd
) bbb
where mcht_cd2 is null
union all
select
    `dt_type`,
    `mcht_cd`,
    `bran_cd`,
    `mcht_name`,
    `mcht_business_name`,
    `mcht_org_cd`,
    `mcht_business_license`,
    `mcht_type`,
    `mcht_service_type`,
    `servcie_org`,
    `reg_prov_cd`,
    `reg_city_cd`,
    `reg_country_cd`,
    `mcht_address`,
    `contact_name`,
    `contact_phone`,
    `contact_fax`,
    `contact_email`,
    `bloc_mcht_cd`,
    `bloc_mcht_name`,
    `mcht_settlement_way`,
    `approve_date`,
    `lose_date`,
    `service_level`,
    `source_from`,
    `referrer`,
    `customer_cd`,
    `customer_name`,
    `mcc_cd`,
    `org_cd`,
    `legal_person_name`,
    `legal_person_id_type`,
    `legal_person_id_no`,
    `mcht_settlement_cycle`,
    `normal_clt_count`,
    `undo_clt_count`,
    `mcht_nature`,
    `mcht_status`,
    `is_risk_lp`,
    `mcht_dept`,
    `has_overdue`,
    `financial_name`,
    `financial_phone`,
    `tax_reg_no`,
    `settlement_model`,
    `settlement_cycle`,
    `settlement_way`,
    `data_channel`,
    `is_group_mcht`,
    `is_point_card_settlement`,
    `customer_no`,
    `area_standard`,
    `reg_capital`,
    `main_business`,
    `scope_of_business`,
    `business_license_limit`,
    `service_nature`,
    `product_info`,
    `mcht_mcc`,
    `three_in_one_flag`,
    `us_credit_code_no`,
    `us_credit_code_start_date`,
    `us_credit_code_end_date`,
    99990101 as `act_end`,
    p_date as `act_start`
 from (
    select *, 
        (
            ( COUNT(*) OVER(PARTITION BY mcht_cd) ) -
            ( ROW_NUMBER() OVER(PARTITION BY mcht_cd) )
        ) AS num 
    from ods_ftp.merchant_update where p_date=${THE_DATE}
) ccc
where ccc.num=0
;


insert into table `dw_2g`.`dwd_merchant_info_history`
    partition (act_end=${THE_DATE})
select
    `dt_type`,
    `mcht_cd`,
    `bran_cd`,
    `mcht_name`,
    `mcht_business_name`,
    `mcht_org_cd`,
    `mcht_business_license`,
    `mcht_type`,
    `mcht_service_type`,
    `servcie_org`,
    `reg_prov_cd`,
    `reg_city_cd`,
    `reg_country_cd`,
    `mcht_address`,
    `contact_name`,
    `contact_phone`,
    `contact_fax`,
    `contact_email`,
    `bloc_mcht_cd`,
    `bloc_mcht_name`,
    `mcht_settlement_way`,
    `approve_date`,
    `lose_date`,
    `service_level`,
    `source_from`,
    `referrer`,
    `customer_cd`,
    `customer_name`,
    `mcc_cd`,
    `org_cd`,
    `legal_person_name`,
    `legal_person_id_type`,
    `legal_person_id_no`,
    `mcht_settlement_cycle`,
    `normal_clt_count`,
    `undo_clt_count`,
    `mcht_nature`,
    `mcht_status`,
    `is_risk_lp`,
    `mcht_dept`,
    `has_overdue`,
    `financial_name`,
    `financial_phone`,
    `tax_reg_no`,
    `settlement_model`,
    `settlement_cycle`,
    `settlement_way`,
    `data_channel`,
    `is_group_mcht`,
    `is_point_card_settlement`,
    `customer_no`,
    `area_standard`,
    `reg_capital`,
    `main_business`,
    `scope_of_business`,
    `business_license_limit`,
    `service_nature`,
    `product_info`,
    `mcht_mcc`,
    `three_in_one_flag`,
    `us_credit_code_no`,
    `us_credit_code_start_date`,
    `us_credit_code_end_date`,
    `act_start`
from `dw_2g`.`dwd_merchant_info`
LEFT SEMI JOIN (
    SELECT mcht_cd from ods_ftp.merchant_update where p_date=${THE_DATE}
) aaa
on dwd_merchant_info.mcht_cd = aaa.mcht_cd
;

drop table if exists dw_2g.dwd_merchant_info;

ALTER TABLE dw_2g.tmp_merchant_${THE_DATE} RENAME TO dw_2g.dwd_merchant_info;
