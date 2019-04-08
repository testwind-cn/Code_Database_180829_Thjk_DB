-- db = pymysql.connect("10.91.1.10", "root", "RiskControl@2018", "unify")

-- db = pymysql.connect("10.91.1.10", "root", "RiskControl@2018", "data_warehouse")


-- 1.信审历史表_credit_history
-- 查询与排序方式
SELECT * FROM
( SELECT   * from credit_history where prod_code in ( '300317','0002000001','9001000001')
  ORDER BY prod_code,CAST(apply_no AS UNSIGNED)
) data1
UNION ALL
SELECT * FROM
(
SELECT   * from credit_history where prod_code in
 ('8004000001', '8005000001','9002000003', '9003000004',
  '9004000004', '9005000001','9006000003', '9007000004')
-- not in ( '1','300317','0002000001','9001000001')
  ORDER BY prod_code,apply_no
) data2;



-- 2.信审流水表_credit_pos_flow
-- 查询与排序方式
SELECT * from credit_pos_flow  ORDER BY merchant_ap;



-- 3.信审风险表_credit_pos_risk
-- 查询与排序方式
SELECT * from credit_pos_risk  ORDER BY merchant_ap;



-- 4.入口贷款表_entry_loan_apply.csv
-- 查询与排序方式
SELECT * from entry_loan_apply  where prod_code <> '1' ORDER BY prod_code,lmt_serno,merchant_ap,credit_status;



-- 5.入口支用表_entry_loan_use.csv
-- 查询与排序方式
SELECT * FROM
( SELECT   * from entry_loan_use where prod_code in ( '300317','0002000001') or prod_code is null
  ORDER BY prod_code,CAST(trim(record_no) AS UNSIGNED)
) data1
UNION ALL
SELECT * FROM
(
SELECT   * from entry_loan_use where prod_code in
 ('8004000001', '8005000001','9002000003', '9003000004',
  '9004000004', '9005000001','9006000003', '9007000004')
-- not in ( '1','300317','0002000001','9001000001')
  ORDER BY prod_code,record_no
) data2;



-- 6.入口商户表_entry_merchant.csv
-- 查询与排序方式
select  * from entry_merchant order by merchant_ap;
-- 8126947



-- 7.入口流水表_entry_pos_flow.csv
-- 查询与排序方式
select  * from entry_pos_flow order by merchant_ap;



-- 8.入口还款计划表_entry_repay_plan.csv
-- 查询与排序方式
SELECT * FROM
( SELECT   * from entry_repay_plan where prod_code in ( '9001000001')
  ORDER BY prod_code,CAST(trim(record_no) AS UNSIGNED)
) data1
UNION ALL
SELECT * FROM
(
SELECT   * from entry_repay_plan where prod_code in
 ('8004000001', '8005000001','9002000003', '9003000004',
  '9004000004', '9005000001','9006000003', '9007000004')
-- not in ( '1','300317','0002000001','9001000001')
  ORDER BY prod_code,record_no
) data2;






-- https://www.jb51.net/article/99842.htm
-- MySQL中union和order by同时使用的实现方法
-- （2）可以通过两个查询分别加括号的方式，改成如下：

-- ------------ 建表语句 ---------------

-- 1.信审历史表_credit_history
-- 建表字段
CREATE TABLE `credit_history` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `apply_no` varchar(100) DEFAULT NULL COMMENT '申请号，资产：loan_credit_apply.record_no、保理：gb_loan_application_kkd.apply_no',
  `merchant_ap` varchar(100) DEFAULT NULL COMMENT '商户号',
  `cert_name` varchar(100) DEFAULT NULL COMMENT '申请人姓名',
  `cert_no` varchar(100) DEFAULT NULL COMMENT '法人证件号',
  `mate_cert_no` varchar(100) DEFAULT NULL COMMENT '配偶证件号码，资产：loan_credit_apply.mate_id_no、保理：空',
  `prod_code` varchar(100) DEFAULT NULL COMMENT '产品代码',
  `loan_amt` varchar(100) DEFAULT NULL COMMENT '贷款金额，\r\n好老板：review_amount>ft_loan_amount> loan_amount\r\n通联宝：effective_amount>bank_amount>apply_amount',
  `credit_begin_date` date DEFAULT NULL COMMENT '贷款开始日期',
  `credit_end_date` date DEFAULT NULL COMMENT '贷款结束日期',
  `apply_status` varchar(10) DEFAULT NULL COMMENT '申请状态',
  `overdue_status` varchar(10) DEFAULT NULL COMMENT '逾期状态，1 逾期、0 正常',
  `is_delete` tinyint(4) DEFAULT NULL COMMENT '逻辑删除，1.是，0.否',
  `create_time` date DEFAULT NULL COMMENT '创建时间',
  `create_user` varchar(100) DEFAULT NULL COMMENT '创建人',
  `modify_time` date DEFAULT NULL COMMENT '修改时间',
  `modify_user` varchar(100) DEFAULT NULL COMMENT '修改人',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=239488 DEFAULT CHARSET=utf8 COMMENT='信审历史表';



/*
-- prod_code ,DISTINCT(LENGTH(apply_no))
-- 0002000001  4 5 6
-- 300317  5 6
-- 8004000001 23
-- 8005000001 23
-- 9001000001 4
-- 9002000003 23
-- 9003000004 23
-- 9004000004 23
-- 9005000001 23
-- 9006000003 23
-- 9007000004 23
*/


-- 2.信审流水表_credit_pos_flow
-- 建表字段
CREATE TABLE `credit_pos_flow` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `flow_type` varchar(25) DEFAULT NULL COMMENT '流水类型，域内 01、域外(支付宝、银联)',
  `merchant_ap` varchar(100) DEFAULT NULL COMMENT '商户号',
  `flow_amt` varchar(250) DEFAULT NULL COMMENT '流水总额（Flow Amount），json示例 {"1M":"30000","2M":"60000","3M":"100000"}',
  `flow_vol` varchar(250) DEFAULT NULL COMMENT '流水笔数（Flow Volume），json示例 {"1M":"50","2M":"60","3M":"100"}',
  `flow_avg_mly` varchar(250) DEFAULT NULL COMMENT '月均流水（Monthly Average Flow Amount），json示例  {"3M":"30000"}',
  `flow_active_days` varchar(250) DEFAULT NULL COMMENT '流水活跃天数（Flow Active Days），json示例 {"1M":"7","3M":"25"}',
  `flow_amt_gt_1w` varchar(250) DEFAULT NULL COMMENT '万元以上流水总额，json示例 {"1M":"10000"}',
  `flow_vol_gt_1w` varchar(250) DEFAULT NULL COMMENT '万元以上流水笔数，json示例 {"1M":"7"}',
  `flow_amt_rate_of_night` varchar(250) DEFAULT NULL COMMENT '夜间交易金额占比（0:00~6:00），json示例  {"1M":"5%"}',
  `flow_amt_rate_of_cc` varchar(250) DEFAULT NULL COMMENT '信用卡刷卡金额占比（卡类型），json示例 {"1M":"8%"}',
  `is_delete` tinyint(4) DEFAULT NULL COMMENT '逻辑删除，1.是，0.否',
  `create_time` date DEFAULT NULL COMMENT '创建日期',
  `create_user` varchar(100) DEFAULT NULL COMMENT '创建人',
  `modify_time` date DEFAULT NULL COMMENT '修改日期',
  `modify_user` varchar(100) DEFAULT NULL COMMENT '修改人',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_createTime_merchantAp` (`create_time`,`merchant_ap`),
  KEY `idx_createTime` (`create_time`),
  KEY `idx_merchantAp` (`merchant_ap`)
) ENGINE=InnoDB AUTO_INCREMENT=1295675 DEFAULT CHARSET=utf8 COMMENT='信审流水表';


-- 3.信审风险表_credit_pos_risk
-- 建表字段
CREATE TABLE `credit_pos_risk` (
  `merchant_ap` varchar(50) DEFAULT NULL COMMENT '商户号',
  `warning_num` varchar(250) DEFAULT NULL COMMENT '警告次数，json示例 {"1M":"20"}',
  `alert_num` varchar(250) DEFAULT NULL COMMENT '预警次数，json示例 {"1M":"30"}',
  `concern_num` varchar(250) DEFAULT NULL COMMENT '关注次数，json示例 {"1M":"7"}',
  `hint_num` varchar(250) DEFAULT NULL COMMENT '提示次数，json示例 {"1M":"11"}',
  `is_delete` tinyint(4) DEFAULT NULL COMMENT '逻辑删除，1.是，0.否',
  `create_time` date DEFAULT NULL COMMENT '创建日期',
  `create_user` varchar(100) DEFAULT NULL COMMENT '创建人',
  `modify_time` date DEFAULT NULL COMMENT '修改日期',
  `modify_user` varchar(100) DEFAULT NULL COMMENT '修改人',
  UNIQUE KEY `uk_createTime_merchantAp` (`create_time`,`merchant_ap`),
  KEY `idx_createTime` (`create_time`),
  KEY `idx_merchantAp` (`merchant_ap`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='信审风险表';


-- 4.入口贷款表_entry_loan_apply
-- 建表字段
CREATE TABLE `entry_loan_apply` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `prod_code` varchar(30) DEFAULT NULL COMMENT '产品编号',
  `merchant_ap` varchar(100) DEFAULT NULL COMMENT '商户号',
  `cert_no` varchar(100) DEFAULT NULL COMMENT '身份证号',
  `lmt_serno` varchar(100) DEFAULT NULL COMMENT '授信合同编号',
  `credit_begin_date` date DEFAULT NULL COMMENT '贷款开始日期',
  `credit_end_date` date DEFAULT NULL COMMENT '贷款结束日期',
  `credit_status` varchar(10) DEFAULT NULL COMMENT '贷款状态',
  `grant_amt` decimal(15,2) DEFAULT NULL COMMENT '贷款总额',
  `available_amt` decimal(15,2) DEFAULT NULL COMMENT '剩余可用额度',
  `loan_term` varchar(30) DEFAULT NULL COMMENT '贷款期限',
  `credit_year_rate` decimal(15,2) DEFAULT NULL COMMENT '贷款年利率',
  `acct_no` varchar(100) DEFAULT NULL COMMENT '当前贷款卡',
  `loan_payway` varchar(30) DEFAULT NULL COMMENT '还款方式',
  `is_delete` tinyint(4) DEFAULT NULL COMMENT '逻辑删除，1.是，0.否',
  `create_time` date DEFAULT NULL COMMENT '创建时间',
  `create_user` varchar(100) DEFAULT NULL COMMENT '创建人',
  `modify_time` date DEFAULT NULL COMMENT '修改时间',
  `modify_user` varchar(100) DEFAULT NULL COMMENT '修改人',
  PRIMARY KEY (`id`),
  KEY `idx_merchantAp` (`merchant_ap`),
  KEY `idx_certNo` (`cert_no`),
  KEY `idx_prodCode` (`prod_code`)
) ENGINE=InnoDB AUTO_INCREMENT=640772 DEFAULT CHARSET=utf8 COMMENT='入口贷款表';


-- 5.入口支用表_entry_loan_use
-- 建表字段
CREATE TABLE `entry_loan_use` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `record_no` varchar(30) DEFAULT NULL COMMENT '支用流水号',
  `prod_code` varchar(30) DEFAULT NULL COMMENT '产品编号',
  `merchant_ap` varchar(100) DEFAULT NULL COMMENT '商户号',
  `cert_no` varchar(100) DEFAULT NULL COMMENT '身份证号',
  `bill_no` varchar(100) DEFAULT NULL COMMENT '借据编号',
  `lmt_serno` varchar(100) DEFAULT NULL COMMENT '授信合同编号',
  `use_begin_date` date DEFAULT NULL COMMENT '支用起始日',
  `use_end_date` date DEFAULT NULL COMMENT '支用到期日',
  `use_amt` decimal(15,2) DEFAULT NULL COMMENT '支用金额',
  `use_status` varchar(10) DEFAULT NULL COMMENT '支用状态',
  `credit_year_rate` decimal(15,2) DEFAULT NULL COMMENT '贷款年利率',
  `loan_term` varchar(30) DEFAULT NULL COMMENT '支用期限',
  `loan_payway` varchar(30) DEFAULT NULL COMMENT '还款方式',
  `repayment_intdate` varchar(8) DEFAULT NULL COMMENT '还息日',
  `repayment_date` varchar(8) DEFAULT NULL COMMENT '还款日',
  `service_fee` decimal(15,2) DEFAULT NULL COMMENT '服务费',
  `manage_fee` decimal(15,2) DEFAULT NULL COMMENT '管理费',
  `is_delete` tinyint(4) DEFAULT NULL COMMENT '逻辑删除，1.是，0.否',
  `create_time` date DEFAULT NULL COMMENT '创建时间',
  `create_user` varchar(100) DEFAULT NULL COMMENT '创建人',
  `modify_time` date DEFAULT NULL COMMENT '修改时间',
  `modify_user` varchar(100) DEFAULT NULL COMMENT '修改人',
  PRIMARY KEY (`id`),
  KEY `idx_merchantAp` (`merchant_ap`),
  KEY `idx_certNo` (`cert_no`),
  KEY `idx_prodCode` (`prod_code`),
  KEY `billNo` (`bill_no`)
) ENGINE=InnoDB AUTO_INCREMENT=147996 DEFAULT CHARSET=utf8 COMMENT='入口支用表 ';

-- --------------------

SELECT DISTINCT(LENGTH(trim(record_no))),prod_code from entry_loan_use  where prod_code ='9007000004';
-- LENGTH(trim(record_no)) ,prod_code
-- 4	= NULL
-- 4,5	= 0002000001
-- 0. = 	1
-- 5	= 300317
-- 23	8004000001
-- 23	8005000001
-- 23	9002000003
-- 23	9003000004
-- 23	9004000004
-- 23	9005000001
-- 23	9006000003
-- 23	9007000004
-- --------------------------------------


-- 6.入口商户表_entry_merchant
-- 建表字段
CREATE TABLE `entry_merchant` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `merchant_ap` varchar(50) NOT NULL DEFAULT '' COMMENT '商户号',
  `merchant_name` varchar(100) DEFAULT NULL COMMENT '商户名 ',
  `business_name` varchar(100) DEFAULT NULL COMMENT '营业名称',
  `cert_name` varchar(100) DEFAULT NULL COMMENT '法人名称',
  `cert_no` varchar(50) DEFAULT NULL COMMENT '法人证件号',
  `uscc` varchar(50) DEFAULT NULL COMMENT '营业执照号(USCC，Uniform Social Credit Code)',
  `mcc` varchar(50) DEFAULT NULL COMMENT '行业分类号(MCC，Merchant Category Code)',
  `is_delete` tinyint(4) DEFAULT NULL COMMENT '逻辑删除，1.是，0.否',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  `create_user` varchar(100) DEFAULT NULL COMMENT '创建人',
  `modify_time` datetime DEFAULT NULL COMMENT '修改时间',
  `modify_user` varchar(100) DEFAULT NULL COMMENT '修改人',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_merchantAp` (`merchant_ap`),
  KEY `idx_uscc` (`uscc`),
  KEY `idx_mcc` (`mcc`),
  KEY `idx_certNo` (`cert_no`),
  KEY `idx_createTime` (`create_time`,`modify_time`),
  KEY `idx_modifyTime` (`modify_time`,`create_time`)
) ENGINE=InnoDB AUTO_INCREMENT=8126948 DEFAULT CHARSET=utf8 COMMENT='入口商户表';


-- 7.入口流水表_bak_190122_entry_pos_flow
-- 建表字段
CREATE TABLE `bak_190122_entry_pos_flow` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `flow_type` varchar(25) DEFAULT NULL COMMENT '流水类型',
  `merchant_ap` varchar(50) DEFAULT NULL COMMENT '商户号',
  `flow_amt` varchar(250) DEFAULT NULL COMMENT '流水总额（Flow Amount），\r\njson示例 {"7D":"1000","1M":"30000","3M":"100000","6M":"200000"}',
  `flow_amt_coord` text COMMENT '流水总额坐标值（Flow Amount Coordinate），json示例\r\n {"1M":["1000","2000","3000","4000"],\r\n"6M":["1000","2000","3000","4000","5000","6000"],\r\n"3M":["1000","2000","3000","4000","5000","6000"],\r\n"7D":["1000","2000","3000","4000","5000","6000","7000"]}',
  `flow_vol` varchar(250) DEFAULT NULL COMMENT '流水笔数（Flow Volume），\r\njson示例 {"7D":"10","1M":"50","3M":"100","6M":"500"}',
  `flow_avg_dly` varchar(250) DEFAULT NULL COMMENT '日均流水（Daily Average Flow Amount），\r\njson示例 {"7D":"100","1M":"90","3M":"120","6M":"110"}',
  `is_delete` tinyint(4) DEFAULT NULL COMMENT '逻辑删除，1.是，0.否',
  `create_time` date DEFAULT NULL COMMENT '创建日期',
  `create_user` varchar(100) DEFAULT NULL COMMENT '创建人',
  `modify_time` date DEFAULT NULL COMMENT '修改日期',
  `modify_user` varchar(100) DEFAULT NULL COMMENT '修改人',
  PRIMARY KEY (`id`),
  KEY `idx_createTime` (`create_time`),
  KEY `idx_merchantAp` (`merchant_ap`)
) ENGINE=InnoDB AUTO_INCREMENT=272127 DEFAULT CHARSET=utf8 COMMENT='入口流水表';


-- 8.入口还款计划表_entry_repay_plan
-- 建表字段
CREATE TABLE `entry_repay_plan` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `source` char(1) DEFAULT NULL COMMENT '来源1-保理2-通华资产',
  `record_no` varchar(30) NOT NULL COMMENT '记录编号',
  `prod_code` varchar(30) DEFAULT NULL COMMENT '产品代码',
  `use_record_no` varchar(30) DEFAULT NULL COMMENT '支用流水号',
  `lmt_serno` varchar(100) DEFAULT NULL COMMENT '授信合同编号',
  `bill_no` varchar(30) DEFAULT NULL COMMENT '借据编号',
  `tpnum` int(11) DEFAULT NULL COMMENT '本期期数',
  `due_date` varchar(8) DEFAULT NULL COMMENT '到期日期',
  `pay_date` varchar(8) DEFAULT NULL COMMENT '还款日',
  `pay_totamt` decimal(15,2) DEFAULT NULL COMMENT '还款总金额',
  `pay_prinamt` decimal(15,2) DEFAULT NULL COMMENT '还款本金',
  `pay_inteamt` decimal(15,2) DEFAULT NULL COMMENT '还款利息',
  `penalty_amt` decimal(15,2) DEFAULT NULL COMMENT '罚息',
  `status` char(2) DEFAULT NULL COMMENT '状态',
  `remark` varchar(500) DEFAULT NULL COMMENT '备注',
  `plan_adm_amt` decimal(15,2) DEFAULT NULL COMMENT '计划服务费金额',
  `paid_amt` decimal(15,2) DEFAULT NULL COMMENT '已还金额',
  `pay_status` varchar(100) DEFAULT NULL COMMENT '还款状态',
  `charge_amt` decimal(15,2) DEFAULT NULL COMMENT '应收费用',
  `data_from` varchar(1) DEFAULT NULL COMMENT '数据来源:1-POS贷2-小微贷3-餐饮贷4-非银贷',
  `is_delete` tinyint(4) DEFAULT NULL COMMENT '逻辑删除，1.是，0.否',
  `create_time` date DEFAULT NULL COMMENT '创建时间',
  `create_user` varchar(100) DEFAULT NULL COMMENT '创建人',
  `modify_time` date DEFAULT NULL COMMENT '修改时间',
  `modify_user` varchar(100) DEFAULT NULL COMMENT '修改人',
  PRIMARY KEY (`id`),
  KEY `idx_use_record_no` (`use_record_no`),
  KEY `idx_lmt_serno` (`lmt_serno`)
) ENGINE=InnoDB AUTO_INCREMENT=244928 DEFAULT CHARSET=utf8 COMMENT='入口还款计划表';

-- -------------------
SELECT DISTINCT(prod_code) from entry_repay_plan;
SELECT DISTINCT(LENGTH(trim(record_no))),prod_code from entry_repay_plan  where prod_code ='9006000003';
-- prod_code = LENGTH(trim(record_no)
-- 9001000001 = 4
-- 8004000001 = 23
-- 8005000001 = 23
-- 9004000004 = 23
-- 9005000001 = 23
-- 9002000003 = 23
-- 9003000004 = 23
-- 9006000003 = 23
-- --------------------------
