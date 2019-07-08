-- 【Hive】 ---------
CREATE TABLE `ods_ftp`.`merchant_info` (
  `mcht_cd`  string COMMENT '商户编码',
  `bran_cd`  string COMMENT '公司代码',
  `mcht_name`  string COMMENT '商户名称',
  `mcht_business_name`  string COMMENT '商户营业名称',
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
  `00_data_00`  string COMMENT '商户开户行省*',
  `00_data_01`  string COMMENT '商户开户行市*',
  `00_data_02`  string COMMENT '商户结算账号*',
  `00_data_03`  string COMMENT '商户结算名称*',
  `00_data_04`  string COMMENT '开户行行号*',
  `00_data_05`  string COMMENT '商户开户行名称*',
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
  `00_data_07`  string COMMENT '开户日期',
  `tax_reg_no`  string COMMENT '税务登记证号',
  `00_data_08`  string COMMENT '商户注册地址',
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
  `tt_type` string COMMENT '[         ]',
  `three_in_one_flag`  string COMMENT '[         ]',
  `us_credit_code_no`  string COMMENT '[         ]',
  `us_credit_code_start_date`  string COMMENT '[         ]',
  `us_credit_code_end_date` string COMMENT '[         ]'

 ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\"",
    "escapeChar" = "\\"
)
STORED AS TEXTFILE;
  


-- 1、删除第一行表头
sed -i '1d' TM_MCHT_INFO.prd.all.del

-- 2、修改字符编码
iconv -f gb18030 -t utf-8  TM_MCHT_INFO.prd.all.del > TM_MCHT_INFO.prd.all_utf.del

-- 3、上传到 HDFS
hadoop fs -put TM_MCHT_INFO.prd.all_utf.del  /user/history/TM_MCHT_INFO.prd.all_utf.del

-- 4、在HIVE 中用上面建表语句建表

-- 5、在 HUE 里加载上面的 HDFS 文件到 HIVE 表中


-- 修改之前的错误字段
three_in_one_flag  -> tt_type
us_credit_code_no -> three_in_one_flag
us_credit_code_start_date -> us_credit_code_no
us_credit_code_end_date -> us_credit_code_start_date
gb_mcht_type -> us_credit_code_end_date






