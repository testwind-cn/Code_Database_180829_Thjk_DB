
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=16000000;

drop table if exists `ods_ftp`.`qsd_merchant_temp`;
CREATE TABLE `ods_ftp`.`qsd_merchant_temp` (
  `flow_amt_1M_1` string COMMENT '',
  `flow_amt_1M_2`  string COMMENT '',
  `flow_amt_6M_1`  string COMMENT '',
  `flow_amt_6M_2`  string COMMENT '',
  `flow_avg_dly_6M`  string COMMENT '',
  `flow_vol_1M`  string COMMENT '',
  `flow_vol_7D`  string COMMENT '',
  `merchant_ap`  string COMMENT ''
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\"",
    "escapeChar" = "\\"
)
STORED AS TEXTFILE;



insert into table `ods_ftp`.`qsd_merchant_temp`
select
    `flow_amt_1M_1`,
    `flow_amt_1M_2`,
    `flow_amt_6M_1`,
    `flow_amt_6M_2`,
    `flow_avg_dly_6M`,
    `flow_vol_1M`,
    `flow_vol_7D`,
    `merchant_ap`
from
(
    select *
    from (
        select
            CAST( regexp_extract(flow_amt_coord,'"1M":\\["([\\d\\.]*)"',1)  AS DECIMAL(30,2)) as flow_amt_1M_1 ,
            CAST( regexp_extract(flow_amt_coord,'"1M":\\["[\\d\\.]*","([\\d\\.]*)"',1) AS DECIMAL(30,2)) as flow_amt_1M_2 ,

            CAST( regexp_extract(flow_amt_coord,'"6M":\\["([\\d\\.]*)"',1)  AS DECIMAL(30,2)) as flow_amt_6M_1 ,
            CAST( regexp_extract(flow_amt_coord,'"6M":\\["[\\d\\.]*","([\\d\\.]*)"',1) AS DECIMAL(30,2)) as flow_amt_6M_2 ,
            CAST( regexp_extract(flow_avg_dly,'"6M":"([\\d\\.]*)',1) AS DECIMAL(30,2)) as flow_avg_dly_6M ,

            CAST( regexp_extract(flow_vol,'"1M":"([\\d\\.]*)',1) AS DECIMAL(30,2)) as flow_vol_1M ,
            CAST( regexp_extract(flow_vol,'"7D":"([\\d\\.]*)',1) AS DECIMAL(30,2)) as flow_vol_7D ,
            entry_pos_flow.merchant_ap
            ,entry_pos_flow.flow_amt_coord
        from dm_unify.entry_pos_flow
        where create_time=current_date()
    ) bbb
    where
        flow_amt_6M_1 > 0 and flow_amt_6M_2 > 0
        and flow_amt_6M_1 >=100 and flow_amt_6M_1 <= 300000
        and flow_avg_dly_6M * 30 >= 100 and flow_avg_dly_6M * 30 <= 300000
        and flow_vol_1M >= 10
        and ( flow_amt_1M_1 + flow_amt_1M_2 ) > 0
        -- and flow_vol_7D >= 1
) ccc
left semi join
(
    SELECT
        regexp_extract( mcht_cd, '[\\s]*([^\\s]*)', 1 ) as mcht_cd_1
    from rds_rc.merchant_zc
    where
        datediff( from_unixtime( unix_timestamp( substr(regexp_extract( legal_iden_no, '[^\\d]*([\\d]*[^\\s]*)',1 ),7,8) ,'yyyyMMdd') , 'yyyy-MM-dd' ), date_sub(current_date(),6574 ) ) <=0
        and
        datediff( from_unixtime( unix_timestamp( substr(regexp_extract( legal_iden_no, '[^\\d]*([\\d]*[^\\s]*)',1 ),7,8) ,'yyyyMMdd') , 'yyyy-MM-dd' ), date_sub(current_date(),20088 ) ) >=0
) aaa
on ccc.merchant_ap = aaa.mcht_cd_1
;


