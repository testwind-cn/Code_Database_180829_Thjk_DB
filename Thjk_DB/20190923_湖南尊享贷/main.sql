
select * from (
    select *
        from ods_nj_posl.apms_mcht
    where trim( apms_mcht.aip_bran_id) = '99995500'
) mmm
--
-- LEFT JOIN
-- (                               -- 机构名称表
--     SELECT inst_name,bran_cd            -- 机构名称，机构编号
--     from
--     rds_rc.organization                 -- 机构表
--     where bran_cd LIKE '99%'            -- 99 开头才是分公司
--         and inst_name like '%分公司'
--         and trim(bran_cd) = '99995500'
-- ) aaa
-- on apms_mcht.aip_bran_id = aaa.bran_cd  -- 机构编号相等

left join
(
    select
        CAST( regexp_extract(flow_avg_dly,'"6M":"([\\d\\.]*)',1) AS DECIMAL(30,2)) as flow_avg_dly_6M ,
        CAST( regexp_extract(flow_amt,'"6M":"([\\d\\.]*)',1) AS DECIMAL(30,2)) as flow_amt_6M ,
        entry_pos_flow.merchant_ap
        ,entry_pos_flow.flow_amt_coord
    from dm_unify.entry_pos_flow
    where create_time=current_date()
 --   and CAST( regexp_extract(flow_avg_dly,'"6M":"([\\d\\.]*)',1) AS DECIMAL(30,2)) * 181 > 600000
    and CAST( regexp_extract(flow_amt,'"6M":"([\\d\\.]*)',1) AS DECIMAL(30,2)) > 600000

) dd
on mmm.mcht_cd=dd.merchant_ap
where dd.merchant_ap is not null