CREATE TABLE `ods_ftp`.`merchant_info` (
  `dt_type` string COMMENT '记录类型', -- HIVE建表
  `mcht_cd`  string COMMENT '商户编码',
  `bran_cd`  string COMMENT '公司代码',
  `mcht_name`  string COMMENT '商户名称  -- 通金导库文件里错写成：商户营业名称*',
  `mcht_business_name`  string COMMENT '商户营业名称  -- 通金导库文件里错写成：商户名称',
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












CREATE TABLE `ods_ftp`.`merchant_info_history` (
  `dt_type` string COMMENT '记录类型', -- HIVE建表
  `mcht_cd`  string COMMENT '商户编码',
  `bran_cd`  string COMMENT '公司代码',
  `mcht_name`  string COMMENT '商户名称  -- 通金导库文件里错写成：商户营业名称*',
  `mcht_business_name`  string COMMENT '商户营业名称  -- 通金导库文件里错写成：商户名称',
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
  `act_start` int COMMENT '生效开始日期'
) PARTITIONED BY ( act_end INT COMMENT '生效结束日期分区')
 ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\"",
    "escapeChar" = "\\"
)
STORED AS TEXTFILE;




-- 第一次把种子数据放入到 merchant_info 表的初始分区(act_start=20150101)
insert into table `ods_ftp`.`merchant_info`
    partition (act_start=20150101)
select
    'NEW' as `dt_type`,
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
    99990101 as `act_end`
from `ods_ftp`.`merchant_info_20190122`;











ALTER TABLE ods_ftp.merchant_info RENAME TO ods_ftp.temp_merchant_20180502;

-- SELECT count(*) from ods_ftp.temp_merchant_20180502;
-- 7713454

-- SELECT count(*) from ods_ftp.merchant_update where p_date=20180502;
-- 3162


/*
SELECT count(*)
from (
    SELECT temp_merchant_20180502.*, aaa.mcht_cd as mcht_cd2
    from ods_ftp.temp_merchant_20180502
    LEFT JOIN (
        select mcht_cd from ods_ftp.merchant_update where p_date=20180502
    ) aaa
    on temp_merchant_20180502.mcht_cd = aaa.mcht_cd
) bbb
where mcht_cd2 is null
*/
-- 7710294
/*
select * from ods_ftp.merchant_update
where p_date=20180502 and ( mcht_cd = '821330159210097' or mcht_cd = '821331054620024' )
*/
-- 有4条


CREATE TABLE `ods_ftp`.`merchant_info` 。。。。。。。。。。。。。。。



TRUNCATE TABLE `ods_ftp`.`merchant_info`;
ALTER TABLE `ods_ftp`.`merchant_info` DROP IF EXISTS PARTITION (act_start=20150101);
ALTER TABLE `ods_ftp`.`merchant_info` DROP IF EXISTS PARTITION (act_start=20180502);


-- 在 Hive 命令行执行
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table `ods_ftp`.`merchant_info`
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
    SELECT temp_merchant_20180502.*, aaa.mcht_cd as mcht_cd2
    from ods_ftp.temp_merchant_20180502
    LEFT JOIN (
        select mcht_cd from ods_ftp.merchant_update where p_date=20180502
    ) aaa
    on temp_merchant_20180502.mcht_cd = aaa.mcht_cd
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
    from ods_ftp.merchant_update where p_date=20180502
) ccc
where ccc.num=0
;



/*
-- SELECT count(*),act_start FROM `ods_ftp`.`merchant_info` GROUP BY `act_start`
*/
/* -- 没有用窗口函数去重的结果，有4个是重复的
-- 3162 里面有 4个重复，应该是2个
 	_c0	act_start
1	3162	20180502
2	7710294	20150101
*/
/* -- 用窗口函数去重的结果，没有重复的
-- 3160 没有重复
 	_c0	act_start
1	3160	20180502
2	7710294	20150101
*/


/*
select * from ods_ftp.merchant_update
where p_date=20180502 and ( mcht_cd = '821330159210097' or mcht_cd = '821331054620024' )
*/
-- 这两个商户号，有4条，我取下面一条使用
/*
UPDATE	821330159210097	99993300	四川隔壁仓库电子商务有限公司	成都市隔壁仓库电子商务有限公司	48213300	915101003946129067	5921	0	99993300	51	01	01	杭州市西湖区文一西路166-2号1室	朱洁	057181020402			65000047	1919（浙江）	0	2017-12-15		2	4	王怡文				915101003946129067	陈江涛	01_身份证	512929197110190016	0_日结	0	0	0_国有	01				朱洁	057181020402	915101003946129067	1	是	0	01	否	否	200033010212222	0	665544	开发销售酒类	开发销售酒类	2099-12-31	01	01	5921	N				20180502	
UPDATE	821330159210097	99993300	四川隔壁仓库电子商务有限公司	成都市隔壁仓库电子商务有限公司	48213300	915101003946129067	5921	0	99993300	51	01	01	杭州市西湖区文一西路166-2号1室	朱洁	057181020402			65000047	1919（浙江）	0	2017-12-15		2	4	王怡文				915101003946129067	陈江涛	01_身份证	512929197110190016	0_日结	0	0	0_国有	01				张灵燕	13956042207	915101003946129067	1	是	0	01	否	否	200033010212222	0	665544	开发销售酒类	开发销售酒类	2099-12-31	01	01	5921	N				20180502	
UPDATE	821331054620024	99993300	临海市乐风烘焙坊	临海市乐风烘焙坊	48213300		5462	0	99993300	33	10	82	古城街道东方大道1号银泰城3F035号	钟先生	18658200762   					0	2017-09-07		2	4	沈东强					许婷婷	01_身份证	331082198411230045	0_日结	0	0	2_个体户	01				钟先生	15068666651		0	是	0	01	否	否	200033100106359	0	0	面包店	面包店		01	01	5462	Y	92331082MA29WTAM6J	2017-05-06	2099-12-31	20180502	
UPDATE	821331054620024	99993300	临海市乐风烘焙坊	临海市乐风烘焙坊	48213300		5462	0	99993300	33	10	82	古城街道东方大道1号银泰城3F035号	钟先生	18658200762   					0	2017-09-07		2	4	沈东强					许婷婷	01_身份证	331082198411230045	0_日结	0	0	2_个体户	01				韩瑜蔚	18658200762   		0	是	0	01	否	否	200033100106359	0	0	面包店	面包店		01	01	5462	Y	92331082MA29WTAM6J	2017-05-06	2099-12-31	20180502	
*/
/*
select *
from (
    select *, 
        (
            ( COUNT(*) OVER(PARTITION BY mcht_cd) ) -
            ( ROW_NUMBER() OVER(PARTITION BY mcht_cd) )
        ) AS num 
    from ods_ftp.merchant_update where p_date=20180502
) aaa
where aaa.num>0
*/
-- aaa.num>0 这两条是排在上面的记录，我只使用 aaa.num=0 的最后一条记录
/*
UPDATE	821330159210097	99993300	四川隔壁仓库电子商务有限公司	成都市隔壁仓库电子商务有限公司	48213300	915101003946129067	5921	0	99993300	51	01	01	杭州市西湖区文一西路166-2号1室	朱洁	057181020402			65000047	1919（浙江）	0	2017-12-15		2	4	王怡文				915101003946129067	陈江涛	01_身份证	512929197110190016	0_日结	0	0	0_国有	01				朱洁	057181020402	915101003946129067	1	是	0	01	否	否	200033010212222	0	665544	开发销售酒类	开发销售酒类	2099-12-31	01	01	5921	N				20180502	1	
UPDATE	821331054620024	99993300	临海市乐风烘焙坊	临海市乐风烘焙坊	48213300		5462	0	99993300	33	10	82	古城街道东方大道1号银泰城3F035号	钟先生	18658200762   					0	2017-09-07		2	4	沈东强					许婷婷	01_身份证	331082198411230045	0_日结	0	0	2_个体户	01				钟先生	15068666651		0	是	0	01	否	否	200033100106359	0	0	面包店	面包店		01	01	5462	Y	92331082MA29WTAM6J	2017-05-06	2099-12-31	20180502	1	
*/

/*
select * from ods_ftp.merchant_update
where  ( mcht_cd = '821330159210097' or mcht_cd = '821331054620024' )
*/
/*
-------- 没排序的
UPDATE	821330159210097	99993300	四川隔壁仓库电子商务有限公司	成都市隔壁仓库电子商务有限公司	48213300	915101003946129067	5921	0	99993300	51	01	01	杭州市西湖区文一西路166-2号1室	朱洁	057181020402			65000047	1919（浙江）	0	2017-12-15		2	4	王怡文				915101003946129067	陈江涛	01_身份证	512929197110190016	0_日结	0	0	0_国有	01				朱洁	057181020402	915101003946129067	1	是	0	01	否	否	200033010212222	0	665544	开发销售酒类	开发销售酒类	2099-12-31	01	01	5921	N				20180502	
UPDATE	821330159210097	99993300	四川隔壁仓库电子商务有限公司	成都市隔壁仓库电子商务有限公司	48213300	915101003946129067	5921	0	99993300	51	01	01	杭州市西湖区文一西路166-2号1室	朱洁	057181020402			65000047	1919（浙江）	0	2017-12-15		2	4	王怡文				915101003946129067	陈江涛	01_身份证	512929197110190016	0_日结	0	0	0_国有	01				张灵燕	13956042207	915101003946129067	1	是	0	01	否	否	200033010212222	0	665544	开发销售酒类	开发销售酒类	2099-12-31	01	01	5921	N				20180502	
UPDATE	821331054620024	99993300	临海市乐风烘焙坊	临海市乐风烘焙坊	48213300		5462	0	99993300	33	10	82	古城街道东方大道1号银泰城3F035号	钟先生	18658200762   					0	2017-09-07		2	4	沈东强					许婷婷	01_身份证	331082198411230045	0_日结	0	0	2_个体户	01				钟先生	15068666651		0	是	0	01	否	否	200033100106359	0	0	面包店	面包店		01	01	5462	Y	92331082MA29WTAM6J	2017-05-06	2099-12-31	20180502	
UPDATE	821331054620024	99993300	临海市乐风烘焙坊	临海市乐风烘焙坊	48213300		5462	0	99993300	33	10	82	古城街道东方大道1号银泰城3F035号	钟先生	18658200762   					0	2017-09-07		2	4	沈东强					许婷婷	01_身份证	331082198411230045	0_日结	0	0	2_个体户	01				韩瑜蔚	18658200762   		0	是	0	01	否	否	200033100106359	0	0	面包店	面包店		01	01	5462	Y	92331082MA29WTAM6J	2017-05-06	2099-12-31	20180502	
UPDATE	821331054620024	99993300	临海市乐风烘焙坊	临海市乐风烘焙坊	48213300		5462	0	99993300	33	10	82	古城街道东方大道1号银泰城3F035号	钟先生	18658200762   					0	2017-09-07		2	4	沈东强					许婷婷	01_身份证	331082198411230045	0_日结	0	0	2_个体户	03				钟先生	15068666651		0	是	0	01	否	否	200033100106359	0	0	面包店	面包店		01	01	5462	Y	92331082MA29WTAM6J	2017-05-06	2099-12-31	20180503	
UPDATE	821331054620024	99993300	临海市乐风烘焙坊	临海市乐风烘焙坊	48213300		5462	0	99993300	33	10	82	古城街道东方大道1号银泰城3F035号	钟先生	18658200762   					0	2017-09-07		2	4	沈东强					许婷婷	01_身份证	331082198411230045	0_日结	0	0	2_个体户	03				韩瑜蔚	18658200762   		0	是	0	01	否	否	200033100106359	0	0	面包店	面包店		01	01	5462	Y	92331082MA29WTAM6J	2017-05-06	2099-12-31	20180503	
UPDATE	821330159210097	99993300	四川隔壁仓库电子商务有限公司	成都市隔壁仓库电子商务有限公司	48213300	915101003946129067	5921	0	99993300	51	01	01	杭州市西湖区文一西路166-2号1室	朱洁	057181020402			65000047	1919（浙江）	0	2017-12-15		2	4	徐丽利				915101003946129067	陈江涛	01_身份证	512929197110190016	0_日结	0	0	0_国有	01				朱洁	057181020402	915101003946129067	1	是	0	01	否	否	200033010212222	0	665544	开发销售酒类	开发销售酒类	2099-12-31	01	01	5921	N				20180815	
UPDATE	821330159210097	99993300	四川隔壁仓库电子商务有限公司	成都市隔壁仓库电子商务有限公司	48213300	915101003946129067	5921	0	99993300	51	01	01	杭州市西湖区文一西路166-2号1室	朱洁	057181020402			65000047	1919（浙江）	0	2017-12-15		2	4	徐丽利				915101003946129067	陈江涛	01_身份证	512929197110190016	0_日结	0	0	0_国有	01				张灵燕	13956042207	915101003946129067	1	是	0	01	否	否	200033010212222	0	665544	开发销售酒类	开发销售酒类	2099-12-31	01	01	5921	N				20180815	
UPDATE	821330159210097	99993300	四川隔壁仓库电子商务有限公司	成都市隔壁仓库电子商务有限公司	48213300	915101003946129067	5921	0	99993300	51	01	01	杭州市西湖区文一西路166-2号1室	朱洁	057181020402			65000047	1919（浙江）	0	2017-12-15		2	4	徐丽利				915101003946129067	陈江涛	01_身份证	512929197110190016	0_日结	0	0	0_国有	01				张灵燕	13956042207	915101003946129067	1	是	0	01	否	否	200033010212222	0	665544	开发销售酒类	开发销售酒类	2099-12-31	01	01	5921	N				20190429	
UPDATE	821330159210097	99993300	四川隔壁仓库电子商务有限公司	成都市隔壁仓库电子商务有限公司	48213300	915101003946129067	5921	0	99993300	51	01	01	杭州市西湖区文一西路166-2号1室	朱洁	057181020402			65000047	1919（浙江）	0	2017-12-15		2	4	王怡文				915101003946129067	陈江涛	01_身份证	512929197110190016	0_日结	0	0	0_国有	01				朱洁	057181020402	915101003946129067	1	是	0	01	否	否	200033010212222	0	665544	开发销售酒类	开发销售酒类	2099-12-31	01	01	5921	N				20180601	
UPDATE	821330159210097	99993300	四川隔壁仓库电子商务有限公司	成都市隔壁仓库电子商务有限公司	48213300	915101003946129067	5921	0	99993300	51	01	01	杭州市西湖区文一西路166-2号1室	朱洁	057181020402			65000047	1919（浙江）	0	2017-12-15		2	4	王怡文				915101003946129067	陈江涛	01_身份证	512929197110190016	0_日结	0	0	0_国有	01				张灵燕	13956042207	915101003946129067	1	是	0	01	否	否	200033010212222	0	665544	开发销售酒类	开发销售酒类	2099-12-31	01	01	5921	N				20180601	
UPDATE	821330159210097	99993300	四川隔壁仓库电子商务有限公司	成都市隔壁仓库电子商务有限公司	48213300	915101003946129067	5921	0	99993300	51	01	01	杭州市西湖区文一西路166-2号1室	朱洁	057181020402			65000047	1919（浙江）	0	2017-12-15		2	4	徐丽利				915101003946129067	陈江涛	01_身份证	512929197110190016	0_日结	0	0	0_国有	01				张灵燕	13956042207	915101003946129067	1	是	0	01	否	否	200033010212222	0	665544	开发销售酒类	开发销售酒类	2099-12-31	01	01	5921	N				20180921	
UPDATE	821330159210097	99993300	四川隔壁仓库电子商务有限公司	成都市隔壁仓库电子商务有限公司	48213300	915101003946129067	5921	0	99993300	51	01	01	杭州市西湖区文一西路166-2号1室	朱洁	057181020402			65000047	1919（浙江）	0	2017-12-15		2	4	陆琴琴				915101003946129067	陈江涛	01_身份证	512929197110190016	0_日结	0	0	0_国有	01				张灵燕	13956042207	915101003946129067	1	是	0	01	否	否	200033010212222	0	665544	开发销售酒类	开发销售酒类	2099-12-31	01	01	5921	N				20190220	
UPDATE	821330159210097	99993300	四川隔壁仓库电子商务有限公司	成都市隔壁仓库电子商务有限公司	48213300	915101003946129067	5921	0	99993300	51	01	01	杭州市西湖区文一西路166-2号1室	朱洁	057181020402			65000047	1919（浙江）	0	2017-12-15		2	4	徐丽利				915101003946129067	陈江涛	01_身份证	512929197110190016	0_日结	0	0	0_国有	01				张灵燕	13956042207	915101003946129067	1	是	0	01	否	否	200033010212222	0	665544	开发销售酒类	开发销售酒类	2099-12-31	01	01	5921	N				20190202	
UPDATE	821330159210097	99993300	四川隔壁仓库电子商务有限公司	成都市隔壁仓库电子商务有限公司	48213300	915101003946129067	5921	0	99993300	51	01	01	杭州市西湖区文一西路166-2号1室	朱洁	057181020402			65000047	1919（浙江）	0	2017-12-15		2	4	徐丽利				915101003946129067	陈江涛	01_身份证	512929197110190016	0_日结	0	0	0_国有	01				张灵燕	13956042207	915101003946129067	1	是	0	01	否	否	200033010212222	0	665544	开发销售酒类	开发销售酒类	2099-12-31	01	01	5921	N				20181217	

-------- 我手工排序的
UPDATE	821330159210097	99993300	四川隔壁仓库电子商务有限公司	成都市隔壁仓库电子商务有限公司	48213300	915101003946129067	5921	0	99993300	51	01	01	杭州市西湖区文一西路166-2号1室	朱洁	057181020402			65000047	1919（浙江）	0	2017-12-15		2	4	王怡文				915101003946129067	陈江涛	01_身份证	512929197110190016	0_日结	0	0	0_国有	01				朱洁	057181020402	915101003946129067	1	是	0	01	否	否	200033010212222	0	665544	开发销售酒类	开发销售酒类	2099-12-31	01	01	5921	N				20180502	
UPDATE	821330159210097	99993300	四川隔壁仓库电子商务有限公司	成都市隔壁仓库电子商务有限公司	48213300	915101003946129067	5921	0	99993300	51	01	01	杭州市西湖区文一西路166-2号1室	朱洁	057181020402			65000047	1919（浙江）	0	2017-12-15		2	4	王怡文				915101003946129067	陈江涛	01_身份证	512929197110190016	0_日结	0	0	0_国有	01				张灵燕	13956042207	915101003946129067	1	是	0	01	否	否	200033010212222	0	665544	开发销售酒类	开发销售酒类	2099-12-31	01	01	5921	N				20180502
	
UPDATE	821330159210097	99993300	四川隔壁仓库电子商务有限公司	成都市隔壁仓库电子商务有限公司	48213300	915101003946129067	5921	0	99993300	51	01	01	杭州市西湖区文一西路166-2号1室	朱洁	057181020402			65000047	1919（浙江）	0	2017-12-15		2	4	王怡文				915101003946129067	陈江涛	01_身份证	512929197110190016	0_日结	0	0	0_国有	01				朱洁	057181020402	915101003946129067	1	是	0	01	否	否	200033010212222	0	665544	开发销售酒类	开发销售酒类	2099-12-31	01	01	5921	N				20180601	
UPDATE	821330159210097	99993300	四川隔壁仓库电子商务有限公司	成都市隔壁仓库电子商务有限公司	48213300	915101003946129067	5921	0	99993300	51	01	01	杭州市西湖区文一西路166-2号1室	朱洁	057181020402			65000047	1919（浙江）	0	2017-12-15		2	4	王怡文				915101003946129067	陈江涛	01_身份证	512929197110190016	0_日结	0	0	0_国有	01				张灵燕	13956042207	915101003946129067	1	是	0	01	否	否	200033010212222	0	665544	开发销售酒类	开发销售酒类	2099-12-31	01	01	5921	N				20180601

UPDATE	821330159210097	99993300	四川隔壁仓库电子商务有限公司	成都市隔壁仓库电子商务有限公司	48213300	915101003946129067	5921	0	99993300	51	01	01	杭州市西湖区文一西路166-2号1室	朱洁	057181020402			65000047	1919（浙江）	0	2017-12-15		2	4	徐丽利				915101003946129067	陈江涛	01_身份证	512929197110190016	0_日结	0	0	0_国有	01				朱洁	057181020402	915101003946129067	1	是	0	01	否	否	200033010212222	0	665544	开发销售酒类	开发销售酒类	2099-12-31	01	01	5921	N				20180815	
UPDATE	821330159210097	99993300	四川隔壁仓库电子商务有限公司	成都市隔壁仓库电子商务有限公司	48213300	915101003946129067	5921	0	99993300	51	01	01	杭州市西湖区文一西路166-2号1室	朱洁	057181020402			65000047	1919（浙江）	0	2017-12-15		2	4	徐丽利				915101003946129067	陈江涛	01_身份证	512929197110190016	0_日结	0	0	0_国有	01				张灵燕	13956042207	915101003946129067	1	是	0	01	否	否	200033010212222	0	665544	开发销售酒类	开发销售酒类	2099-12-31	01	01	5921	N				20180815

UPDATE	821330159210097	99993300	四川隔壁仓库电子商务有限公司	成都市隔壁仓库电子商务有限公司	48213300	915101003946129067	5921	0	99993300	51	01	01	杭州市西湖区文一西路166-2号1室	朱洁	057181020402			65000047	1919（浙江）	0	2017-12-15		2	4	徐丽利				915101003946129067	陈江涛	01_身份证	512929197110190016	0_日结	0	0	0_国有	01				张灵燕	13956042207	915101003946129067	1	是	0	01	否	否	200033010212222	0	665544	开发销售酒类	开发销售酒类	2099-12-31	01	01	5921	N				20180921	
UPDATE	821330159210097	99993300	四川隔壁仓库电子商务有限公司	成都市隔壁仓库电子商务有限公司	48213300	915101003946129067	5921	0	99993300	51	01	01	杭州市西湖区文一西路166-2号1室	朱洁	057181020402			65000047	1919（浙江）	0	2017-12-15		2	4	徐丽利				915101003946129067	陈江涛	01_身份证	512929197110190016	0_日结	0	0	0_国有	01				张灵燕	13956042207	915101003946129067	1	是	0	01	否	否	200033010212222	0	665544	开发销售酒类	开发销售酒类	2099-12-31	01	01	5921	N				20181217	
UPDATE	821330159210097	99993300	四川隔壁仓库电子商务有限公司	成都市隔壁仓库电子商务有限公司	48213300	915101003946129067	5921	0	99993300	51	01	01	杭州市西湖区文一西路166-2号1室	朱洁	057181020402			65000047	1919（浙江）	0	2017-12-15		2	4	徐丽利				915101003946129067	陈江涛	01_身份证	512929197110190016	0_日结	0	0	0_国有	01				张灵燕	13956042207	915101003946129067	1	是	0	01	否	否	200033010212222	0	665544	开发销售酒类	开发销售酒类	2099-12-31	01	01	5921	N				20190202	
UPDATE	821330159210097	99993300	四川隔壁仓库电子商务有限公司	成都市隔壁仓库电子商务有限公司	48213300	915101003946129067	5921	0	99993300	51	01	01	杭州市西湖区文一西路166-2号1室	朱洁	057181020402			65000047	1919（浙江）	0	2017-12-15		2	4	陆琴琴				915101003946129067	陈江涛	01_身份证	512929197110190016	0_日结	0	0	0_国有	01				张灵燕	13956042207	915101003946129067	1	是	0	01	否	否	200033010212222	0	665544	开发销售酒类	开发销售酒类	2099-12-31	01	01	5921	N				20190220	
UPDATE	821330159210097	99993300	四川隔壁仓库电子商务有限公司	成都市隔壁仓库电子商务有限公司	48213300	915101003946129067	5921	0	99993300	51	01	01	杭州市西湖区文一西路166-2号1室	朱洁	057181020402			65000047	1919（浙江）	0	2017-12-15		2	4	徐丽利				915101003946129067	陈江涛	01_身份证	512929197110190016	0_日结	0	0	0_国有	01				张灵燕	13956042207	915101003946129067	1	是	0	01	否	否	200033010212222	0	665544	开发销售酒类	开发销售酒类	2099-12-31	01	01	5921	N				20190429	
-- ----------------
UPDATE	821331054620024	99993300	临海市乐风烘焙坊	临海市乐风烘焙坊	48213300		5462	0	99993300	33	10	82	古城街道东方大道1号银泰城3F035号	钟先生	18658200762   					0	2017-09-07		2	4	沈东强					许婷婷	01_身份证	331082198411230045	0_日结	0	0	2_个体户	01				钟先生	15068666651		0	是	0	01	否	否	200033100106359	0	0	面包店	面包店		01	01	5462	Y	92331082MA29WTAM6J	2017-05-06	2099-12-31	20180502	
UPDATE	821331054620024	99993300	临海市乐风烘焙坊	临海市乐风烘焙坊	48213300		5462	0	99993300	33	10	82	古城街道东方大道1号银泰城3F035号	钟先生	18658200762   					0	2017-09-07		2	4	沈东强					许婷婷	01_身份证	331082198411230045	0_日结	0	0	2_个体户	01				韩瑜蔚	18658200762   		0	是	0	01	否	否	200033100106359	0	0	面包店	面包店		01	01	5462	Y	92331082MA29WTAM6J	2017-05-06	2099-12-31	20180502	

UPDATE	821331054620024	99993300	临海市乐风烘焙坊	临海市乐风烘焙坊	48213300		5462	0	99993300	33	10	82	古城街道东方大道1号银泰城3F035号	钟先生	18658200762   					0	2017-09-07		2	4	沈东强					许婷婷	01_身份证	331082198411230045	0_日结	0	0	2_个体户	03				钟先生	15068666651		0	是	0	01	否	否	200033100106359	0	0	面包店	面包店		01	01	5462	Y	92331082MA29WTAM6J	2017-05-06	2099-12-31	20180503	
UPDATE	821331054620024	99993300	临海市乐风烘焙坊	临海市乐风烘焙坊	48213300		5462	0	99993300	33	10	82	古城街道东方大道1号银泰城3F035号	钟先生	18658200762   					0	2017-09-07		2	4	沈东强					许婷婷	01_身份证	331082198411230045	0_日结	0	0	2_个体户	03				韩瑜蔚	18658200762   		0	是	0	01	否	否	200033100106359	0	0	面包店	面包店		01	01	5462	Y	92331082MA29WTAM6J	2017-05-06	2099-12-31	20180503		
*/
----------------------------- OK 


insert into table `ods_ftp`.`merchant_info_history`
    partition (act_end=20180502)
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
from `ods_ftp`.`temp_merchant_20180502`
LEFT SEMI JOIN (
    SELECT mcht_cd from ods_ftp.merchant_update where p_date=20180502
) aaa
on temp_merchant_20180502.mcht_cd = aaa.mcht_cd
;


