CREATE TABLE allinpal_rpt.`thbl_rpt_d0009` (
  `col_01` int COMMENT '序号',
  `col_02` string COMMENT '机构号',
  `col_03` string COMMENT '分公司',
  `col_04` string COMMENT '商户号',
  `col_05` string COMMENT '商户名称',
  `col_06` string COMMENT '法人姓名',
  `col_07` string COMMENT '借据号',
  `col_08` date COMMENT '贷款起息日',
  `col_09` date COMMENT '贷款结束日',
  `col_10` decimal(15,4) COMMENT '贷款本金',
  `col_11` int COMMENT '贷款期限',
  `col_12` decimal(9,6) COMMENT '贷款利率',
  `col_13` decimal(9,6) COMMENT '罚息利率',
  `col_14` decimal(9,6) COMMENT '滞纳金利率',
  `col_15` string COMMENT '还款方式（日/周/双周/月）',
  `col_16` decimal(15,4) COMMENT '累计应收金额.本金',
  `col_17` decimal(15,4) COMMENT '累计应收金额.本金借记调整',
  `col_18` decimal(15,4) COMMENT '累计应收金额.本金贷记调整',
  `col_19` decimal(15,4) COMMENT '累计应收金额.账户管理费1',
  `col_20` decimal(15,4) COMMENT '累计应收金额.账户管理费1借记调整',
  `col_21` decimal(15,4) COMMENT '累计应收金额.账户管理费1贷记调整（表内）',
  `col_22` decimal(15,4) COMMENT '累计应收金额.账户管理费2',
  `col_23` decimal(15,4) COMMENT '累计应收金额.账户管理费2借记调整',
  `col_24` decimal(15,4) COMMENT '累计应收金额.账户管理费2贷记调整（表内）',
  `col_25` decimal(15,4) COMMENT '累计应收金额.滞纳金',
  `col_26` decimal(15,4) COMMENT '累计应收金额.滞纳金借记调整',
  `col_27` decimal(15,4) COMMENT '累计应收金额.滞纳金贷记调整（表内）',
  `col_28` decimal(15,4) COMMENT '累计应收金额.其他或有转入:账户管理费1',
  `col_29` decimal(15,4) COMMENT '累计应收金额.其他或有转入:账户管理费2',
  `col_30` decimal(15,4) COMMENT '累计应收金额.其他或有转入:滞纳金',
  `col_31` decimal(15,4) COMMENT '累计应收金额.合计',
  `col_32` decimal(15,4) COMMENT '累计已还金额.本金',
  `col_33` decimal(15,4) COMMENT '累计已还金额.账户管理费1',
  `col_34` decimal(15,4) COMMENT '累计已还金额.账户管理费2',
  `col_35` decimal(15,4) COMMENT '累计已还金额.滞纳金',
  `col_36` decimal(15,4) COMMENT '累计已还金额.其他或有转入:账户管理费1',
  `col_37` decimal(15,4) COMMENT '累计已还金额.其他或有转入:账户管理费2',
  `col_38` decimal(15,4) COMMENT '累计已还金额.其他或有转入:滞纳金',
  `col_39` decimal(15,4) COMMENT '累计已还金额.合计',
  `col_40` decimal(15,4) COMMENT '贷款余额.本金',
  `col_41` decimal(15,4) COMMENT '贷款余额.账户管理费1',
  `col_42` decimal(15,4) COMMENT '贷款余额.账户管理费2',
  `col_43` decimal(15,4) COMMENT '贷款余额.滞纳金',
  `col_44` decimal(15,4) COMMENT '贷款余额.合计',
  `col_45` string COMMENT '贷款余额.贷款状态',
  `col_46` string COMMENT '是否逾期',
  `col_47` int COMMENT '逾期天数'
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