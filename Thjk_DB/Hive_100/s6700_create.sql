CREATE TABLE allinpal_rpt.`thbl_rpt_s6700` (
  `col_01` string COMMENT '机构号ORG_NO',
  `col_02` string COMMENT 'SERIAL',
  `col_03` string COMMENT '产品编号PRODUCT',
  `col_04` string COMMENT '产品描述PRD_DESC',
  `col_05` string COMMENT '商户号MCHT_CD',
  `col_06` string COMMENT '商户名称MCHT_NAME',
  `col_07` string COMMENT '法人姓名STLM_NM',
  `col_08` string COMMENT '贷款编号DB_NO',
  `col_09` string COMMENT '周期类型PERIOD_TYPE',
  `col_10` date COMMENT '借款日期LOAN_DATE',
  `col_11` date COMMENT '到期日期LOAN_MATURITY_DATE',
  `col_12` int COMMENT '贷款时长（天）PERIOD',
  `col_13` decimal(15,4) COMMENT '贷款本金LOAN_PRINCIPAL',
  `col_14` int COMMENT '贷款总期数LOAN_PERIOD_NUM',
  `col_15` string COMMENT '还款方式REPAYMENT_METHOD',
  `col_16` decimal(15,4) COMMENT 'SP_PRINCIPAL',
  `col_17` decimal(15,4) COMMENT 'SP_FEE',
  `col_18` decimal(15,4) COMMENT 'SP_FEE1',
  `col_19` decimal(15,4) COMMENT 'SP_FEE2',
  `col_20` decimal(15,4) COMMENT 'MIN_PAY',
  `col_21` decimal(15,4) COMMENT 'LOAN_BALANCE',
  `col_22` int COMMENT 'OVERDUE_PERIOD',
  `col_23` string COMMENT 'STATUS',
  `col_24` decimal(9,6) COMMENT 'RATE',
  `col_25` decimal(9,6) COMMENT 'RATE1',
  `col_26` decimal(9,6) COMMENT 'RATE2',
  `col_27` int COMMENT 'CURR_PERIOD',
  `col_28` int COMMENT 'OVERDUE_DAYS',
  `col_29` int COMMENT 'OVERDUE_2'
)
PARTITIONED BY (
    `data_dt` date COMMENT '报表日期'
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'colelction.delim'='\u0002',
  'field.delim'='\t',
  'mapkey.delim'='\u0003',
  'serialization.format'='\t')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
;
