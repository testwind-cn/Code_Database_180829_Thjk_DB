
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=16000000;

drop table if exists `dm_2g`.`tmp_dm_qsd_merchant`;
CREATE TABLE `dm_2g`.`tmp_dm_qsd_merchant` (
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



insert into table `dm_2g`.`tmp_dm_qsd_merchant`
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
(   -- 一、取出流水数量符合条件的商户
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
        flow_amt_6M_1 > 0 and flow_amt_6M_2 > 0                                 -- 1、最近2个月每个月有经营流水
        and flow_avg_dly_6M * 30 >= 100 and flow_avg_dly_6M * 30 <= 300000     -- 2、最近6个月月均流水不小于100元，不大于30万元
        -- and flow_amt_6M_1 >=100 and flow_amt_6M_1 <= 300000                     -- 3、最近30天总流水不小于100元，不大于30万元
        -- and flow_vol_1M >= 10                                                   -- 4、最近30天交易笔数不小于10笔
        -- and ( flow_amt_1M_1 + flow_amt_1M_2 ) > 0                               -- 5、最近10天交易笔数不小于1笔         14天
        -- and flow_vol_7D >= 1                                                    -- 5、最近10天交易笔数不小于1笔         7天
) ccc
left semi join
(   -- 二、取出年龄范围在18-55岁的商户，非禁入行业，不在黑名单
    SELECT
        regexp_extract( mcht_cd, '[\\s]*([^\\s]*)', 1 ) as mcht_cd_1
        -- ,regexp_extract( merchant_zc.legal_iden_no, '[^\\d]*([^\\s]*)',1 ) nnn
        -- ,merchant_zc.*
    from rds_rc.merchant_zc
    left join (
            -- 1、从黑名单表里，去除唯一的身份证号码
        SELECT DISTINCT(regexp_extract( cert_number, '[^\\d]*([^\\s]*)',1 ))  as legal_iden_no from dm_unify.black_list
    ) blk_lst
    on
        regexp_extract( merchant_zc.legal_iden_no, '[^\\d]*([^\\s]*)',1 ) = blk_lst.legal_iden_no
    where
            -- 2、18-55岁。            -- 从第一个数字开始，到第一个空格结束 '[^\\d]*([^\\s]*)'         从第7位取8个字符，就是年月日
        datediff( from_unixtime( unix_timestamp( substr(regexp_extract( merchant_zc.legal_iden_no, '[^\\d]*([^\\s]*)',1 ),7,8) ,'yyyyMMdd') , 'yyyy-MM-dd' ), date_sub(current_date(),6574 ) ) <=0
        and
        datediff( from_unixtime( unix_timestamp( substr(regexp_extract( merchant_zc.legal_iden_no, '[^\\d]*([^\\s]*)',1 ),7,8) ,'yyyyMMdd') , 'yyyy-MM-dd' ), date_sub(current_date(),20088 ) ) >=0
            -- 3、不在禁入行业
        and merchant_zc.up_mcc_cd not in (
            3998,4011,4111,4112,4119,4121,4131,4411,4457,4468,
            4511,4582,4899,4900,7297,7298,7911,8651,8661,9211,
            9222,9223,9311,9399,9400,9402
        )
            -- 4、不在黑名单里          总共 6508742 户， 在黑名单的 534846 户 (is not null)，不在黑名单的 5973896 户 (is null)
        and blk_lst.legal_iden_no is null
    -- order by nnn
) aaa
on ccc.merchant_ap = aaa.mcht_cd_1
;




