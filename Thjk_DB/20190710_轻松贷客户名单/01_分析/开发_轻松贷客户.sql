with
aaa as
(
    SELECT
        regexp_extract( mcht_cd, '[\\s]*([^\\s]*)', 1 ) as mcht_cd_1                -- 5506620445803Q6
        -- ,substr(regexp_extract( legal_iden_no, '[^\\d]*([\\d]*[^\\s]*)',1 ),7,8)    -- STR '19860921'
        -- ,unix_timestamp('20190506','yyyyMMdd')                                      -- INT 1557072000
        -- ,from_unixtime( unix_timestamp('20190506','yyyyMMdd') , 'yyyy-MM-dd' )      -- STR '1986-09-21'
        -- ,datediff( from_unixtime( unix_timestamp( substr(regexp_extract( legal_iden_no, '[^\\d]*([\\d]*[^\\s]*)',1 ),7,8) ,'yyyyMMdd') , 'yyyy-MM-dd' ), date_sub(current_date(),CAST(365.24219*18 as INT) ) ) as date_1
                                                                                    -- -5406
        -- ,datediff( from_unixtime( unix_timestamp( substr(regexp_extract( legal_iden_no, '[^\\d]*([\\d]*[^\\s]*)',1 ),7,8) ,'yyyyMMdd') , 'yyyy-MM-dd' ), date_sub(current_date(),CAST(365.24219*55 as INT) ) ) as date_2
                                                                                    -- 8108
        -- ,date_sub(current_date(),CAST(365.24219*18 as INT) )                        -- DATE 2001-07-10
        -- ,date_sub(current_date(),CAST(365.24219*55 as INT) )                        -- DATE 1964-07-10
        -- ,*
    from rds_rc.merchant_zc
    where
        datediff( from_unixtime( unix_timestamp( substr(regexp_extract( legal_iden_no, '[^\\d]*([\\d]*[^\\s]*)',1 ),7,8) ,'yyyyMMdd') , 'yyyy-MM-dd' ), date_sub(current_date(),CAST(365.24219*18 as INT) ) ) <=0
        and
        datediff( from_unixtime( unix_timestamp( substr(regexp_extract( legal_iden_no, '[^\\d]*([\\d]*[^\\s]*)',1 ),7,8) ,'yyyyMMdd') , 'yyyy-MM-dd' ), date_sub(current_date(),CAST(365.24219*55 as INT) ) ) >=0
/*
  34984 小于18岁的
 763274 大于55岁的

 798258 小于18岁的、大于55岁的
6811543 介于18-55岁
1454282 生日 is null
9064083 总数
 */
),
ccc as
(
    select *
    from (
        select
            CAST( regexp_extract(flow_amt_coord,'"1M":\\["([\\d\\.]*)"',1)  AS DECIMAL(30,2)) as flow_amt_1M_1 , -- `最近1-7天交易总额`,
            CAST( regexp_extract(flow_amt_coord,'"1M":\\["[\\d\\.]*","([\\d\\.]*)"',1) AS DECIMAL(30,2)) as flow_amt_1M_2 , -- `最近8-14天交易总额`,

            CAST( regexp_extract(flow_amt_coord,'"6M":\\["([\\d\\.]*)"',1)  AS DECIMAL(30,2)) as flow_amt_6M_1 , -- `最近1-31天交易总额`,
            CAST( regexp_extract(flow_amt_coord,'"6M":\\["[\\d\\.]*","([\\d\\.]*)"',1) AS DECIMAL(30,2)) as flow_amt_6M_2 , -- `最近32-61天交易总额`,
            CAST( regexp_extract(flow_avg_dly,'"6M":"([\\d\\.]*)',1) AS DECIMAL(30,2)) as flow_avg_dly_6M , -- `最近1-181天流水总额 除以 181`,

            CAST( regexp_extract(flow_vol,'"1M":"([\\d\\.]*)',1) AS DECIMAL(30,2)) as flow_vol_1M , -- `最近1-31天流水笔数`,
            CAST( regexp_extract(flow_vol,'"7D":"([\\d\\.]*)',1) AS DECIMAL(30,2)) as flow_vol_7D , -- `最近1-7天流水笔数`,
            entry_pos_flow.merchant_ap
            ,entry_pos_flow.flow_amt
            ,entry_pos_flow.flow_amt_coord
            ,entry_pos_flow.flow_vol
            ,entry_pos_flow.flow_avg_dly
        from dm_unify.entry_pos_flow
        where create_time=current_date()

    ) bbb
    where
        flow_amt_6M_1 > 0 and flow_amt_6M_2 > 0                                     -- 最近1-31天流水总额>0 ， 且， 最近 32-61天流水总额>0
        and flow_amt_6M_1 >=100 and flow_amt_6M_1 <= 300000                         -- 最近1-31天流水总额  >=100 ，且 <= 30W
        and flow_avg_dly_6M * 30 >= 100 and flow_avg_dly_6M * 30 <= 300000          -- 最近1-181天日均流水 乘以30 ，>= 100 ，且 <= 30W
        and flow_vol_1M >= 10                                                       -- 最近1-31天流水笔数 >=10
        and ( flow_amt_1M_1 + flow_amt_1M_2 ) > 0                                   -- 最近1-7天交易总额 +  最近8-14天交易总额  > 0
        -- and flow_vol_7D >= 1
    -- order by flow_avg_dly_6M desc
)
select *
from ccc
left semi join aaa
on ccc.merchant_ap = aaa.mcht_cd_1
;


/*
 	_c0	_c1	_c2	_c3	_c4	_c5	_c6	merchant_zc.entity_oid	merchant_zc.inst_oid	merchant_zc.mcht_cd	merchant_zc.aip_bran_id	merchant_zc.name	merchant_zc.name_busi	merchant_zc.up_bc_cd	merchant_zc.busi_lice_no	merchant_zc.up_mcc_cd	merchant_zc.prov_cd	merchant_zc.city_cd	merchant_zc.area_cd	merchant_zc.busi_addr	merchant_zc.contact	merchant_zc.contact_tel	merchant_zc.fax	merchant_zc.email	merchant_zc.group_id	merchant_zc.appr_date	merchant_zc.delete_date	merchant_zc.dvp_by	merchant_zc.stlm_ins_prov	merchant_zc.stlm_ins_city	merchant_zc.stlm_acct	merchant_zc.stlm_nm	merchant_zc.stlm_ins_cd	merchant_zc.stlm_ins_nm	merchant_zc.status	merchant_zc.remark	merchant_zc.version	merchant_zc.whenmodified	merchant_zc.orig_flag	merchant_zc.flag_deleted	merchant_zc.created_by	merchant_zc.created_date	merchant_zc.last_upd_by	merchant_zc.last_upd_date	merchant_zc.modi_num	merchant_zc.rec_digi_proof	merchant_zc.broker_name	merchant_zc.broker_tel_no	merchant_zc.broker_email	merchant_zc.broker_iden_no	merchant_zc.manager_oid	merchant_zc.organization_code	merchant_zc.legal_name	merchant_zc.legal_iden_type	merchant_zc.legal_iden_no	merchant_zc.settlement_cycle	merchant_zc.merchant_mcc	merchant_zc.normal_term_num	merchant_zc.rev_term_num	merchant_zc.merchant_nature	merchant_zc.merchant_status	merchant_zc.legal_is_risk	merchant_zc.merchant_debts	merchant_zc.principal_is_debts	merchant_zc.finance_name	merchant_zc.finance_hp_no	merchant_zc.open_date	merchant_zc.tax_no	merchant_zc.register_addr
1	19860921	1557072000	2019-05-06	-5406	8108	2001-07-10	1964-07-10	+0000000000000000000000003194818.	+0000000000000000000000000007012.	5506620445803Q6	99996500
2	19680301	1557072000	2019-05-06	-12184	1330	2001-07-10	1964-07-10	+0000000000000000000000003194819.	+0000000000000000000000000007012.	55066204722076M	99996500
3	19700929	1557072000	2019-05-06	-11242	2272	2001-07-10	1964-07-10	+0000000000000000000000003194820.	+0000000000000000000000000007012.	5506620478906U4	99996500
4	19771015	1557072000	2019-05-06	-8669	4845	2001-07-10	1964-07-10	+0000000000000000000000003194821.	+0000000000000000000000000007012.	5506620481203TB	99996500
5	19770513	1557072000	2019-05-06	-8824	4690	2001-07-10	1964-07-10	+0000000000000000000000003194822.	+0000000000000000000000000007012.	550662048120673	99996500
 */




