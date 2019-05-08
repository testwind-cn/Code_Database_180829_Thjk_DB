
SELECT count(*),data_dt from thbl_rpt_d0009 GROUP BY data_dt   having count(*) > 3000 and count(*) < 6000 ORDER BY data_dt;
SELECT count(*),data_dt from thbl_rpt_d0010 GROUP BY data_dt   having count(*) > 7000 and count(*) < 14000 ORDER BY data_dt;
SELECT count(*),data_dt from thbl_rpt_s6700 GROUP BY data_dt   having count(*) > 3000 and count(*) < 6000 ORDER BY data_dt;



-- 纯文本方式的保存，缺陷：
-- 1、不支持转义符，
-- 2、不支持引号，
-- 所以用 \t 做间隔，字段里用什么文本都可以
CREATE TABLE `allinpal_rpt.www2`(
  `field_1` int COMMENT '我我66',
  `field_2` bigint,
  `field_3` string,
  `field_4` string)
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


-- CSV 的方式缺陷太大
-- 1、不能加注释
-- 2、不能设置字段类型
CREATE TABLE `allinpal_rpt.www`(
  `field_1` string COMMENT 'w收到收到',
  `field_2` string COMMENT 'fr收到收到',
  `field_3` string COMMENT 'from deserializer',
  `field_4` string COMMENT 'from 收到收到')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'escapeChar'='\\',
  'quoteChar'='\"',
  'separatorChar'=',')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
  ;
