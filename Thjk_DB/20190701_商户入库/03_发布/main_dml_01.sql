














set mapreduce.job.reduces=20;

SET hive.enforce.bucketing = false;

set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=4096;




set hive.exec.compress.output=true;

set mapred.output.compression.type=BLOCK;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;



SET hive.exec.dynamic.partition=true;

SET hive.exec.dynamic.partition.mode=nonstrict;

SET hive.exec.max.dynamic.partitions.pernode = 4000;

SET hive.exec.max.dynamic.partitions=4000;






















drop table if exists dim.tmp_merchant_update;

create temporary table dim.tmp_merchant_update as
select
    *
from (
    select
        if( INPUT__FILE__NAME like '%SYB%', 10002, 10001) as corp_id
        ,(
            ( COUNT(*) OVER(PARTITION BY mcht_cd, INPUT__FILE__NAME) ) -
            ( ROW_NUMBER() OVER(PARTITION BY mcht_cd, INPUT__FILE__NAME) )
        ) AS num
        ,*
    from ods_ftp.merchant_update
    where p_date=${hivevar:THE_DATE}

) tmp_tb
where num=0
;

drop table if exists dim.tmp_merchant_add_replace;

create temporary table dim.tmp_merchant_add_replace as
select
    cast( row_number() OVER() + ttt2.max_id as bigint ) as merchant_id
    ,tmp_merchant_update.*
from dim.tmp_merchant_update
left join dim.dim_merchant_info
on
    tmp_merchant_update.corp_id = dim_merchant_info.corp_id
    and tmp_merchant_update.mcht_cd = dim_merchant_info.mcht_cd
left join
(
    select
        coalesce(max(cast(merchant_id as bigint)),0) as max_id
    from dim.dim_merchant_info
) ttt2
where
    dim_merchant_info.mcht_cd is null
union all
select
    dim_merchant_info.merchant_id
    ,tmp_merchant_update.*
from dim.tmp_merchant_update
left join dim.dim_merchant_info
on
    tmp_merchant_update.corp_id = dim_merchant_info.corp_id
    and tmp_merchant_update.mcht_cd = dim_merchant_info.mcht_cd
where
    dim_merchant_info.mcht_cd is not null
;



insert into table dim.tmp_dim_merchant_info
    partition (corp_id)
select
    merchant_id,
    act_start,
    act_end,
    dt_type,
    mcht_cd,
    bran_cd,
    mcht_name,
    mcht_business_name,
    mcht_org_cd,
    mcht_business_license,
    mcht_type,
    mcht_service_type,
    servcie_org,
    reg_prov_cd,
    reg_city_cd,
    reg_country_cd,
    mcht_address,
    contact_name,
    contact_phone,
    contact_fax,
    contact_email,
    bloc_mcht_cd,
    bloc_mcht_name,
    mcht_settlement_way,
    approve_date,
    lose_date,
    service_level,
    source_from,
    referrer,
    customer_cd,
    customer_name,
    mcc_cd,
    org_cd,
    legal_person_name,
    legal_person_id_type,
    legal_person_id_no,
    mcht_settlement_cycle,
    normal_clt_count,
    undo_clt_count,
    mcht_nature,
    mcht_status,
    is_risk_lp,
    mcht_dept,
    has_overdue,
    financial_name,
    financial_phone,
    tax_reg_no,
    settlement_model,
    settlement_cycle,
    settlement_way,
    data_channel,
    is_group_mcht,
    is_point_card_settlement,
    customer_no,
    area_standard,
    reg_capital,
    main_business,
    scope_of_business,
    business_license_limit,
    service_nature,
    product_info,
    mcht_mcc,
    three_in_one_flag,
    us_credit_code_no,
    us_credit_code_start_date,
    us_credit_code_end_date,
    corp_id
from (
    select
        main_t.merchant_id,
        main_t.act_start,
        main_t.act_end,
        main_t.dt_type,
        main_t.mcht_cd,
        main_t.bran_cd,
        main_t.mcht_name,
        main_t.mcht_business_name,
        main_t.mcht_org_cd,
        main_t.mcht_business_license,
        main_t.mcht_type,
        main_t.mcht_service_type,
        main_t.servcie_org,
        main_t.reg_prov_cd,
        main_t.reg_city_cd,
        main_t.reg_country_cd,
        main_t.mcht_address,
        main_t.contact_name,
        main_t.contact_phone,
        main_t.contact_fax,
        main_t.contact_email,
        main_t.bloc_mcht_cd,
        main_t.bloc_mcht_name,
        main_t.mcht_settlement_way,
        main_t.approve_date,
        main_t.lose_date,
        main_t.service_level,
        main_t.source_from,
        main_t.referrer,
        main_t.customer_cd,
        main_t.customer_name,
        main_t.mcc_cd,
        main_t.org_cd,
        main_t.legal_person_name,
        main_t.legal_person_id_type,
        main_t.legal_person_id_no,
        main_t.mcht_settlement_cycle,
        main_t.normal_clt_count,
        main_t.undo_clt_count,
        main_t.mcht_nature,
        main_t.mcht_status,
        main_t.is_risk_lp,
        main_t.mcht_dept,
        main_t.has_overdue,
        main_t.financial_name,
        main_t.financial_phone,
        main_t.tax_reg_no,
        main_t.settlement_model,
        main_t.settlement_cycle,
        main_t.settlement_way,
        main_t.data_channel,
        main_t.is_group_mcht,
        main_t.is_point_card_settlement,
        main_t.customer_no,
        main_t.area_standard,
        main_t.reg_capital,
        main_t.main_business,
        main_t.scope_of_business,
        main_t.business_license_limit,
        main_t.service_nature,
        main_t.product_info,
        main_t.mcht_mcc,
        main_t.three_in_one_flag,
        main_t.us_credit_code_no,
        main_t.us_credit_code_start_date,
        main_t.us_credit_code_end_date,
        main_t.corp_id
    from dim.dim_merchant_info as main_t
    LEFT JOIN dim.tmp_merchant_add_replace
    on
        main_t.mcht_cd = tmp_merchant_add_replace.mcht_cd
        and main_t.corp_id = tmp_merchant_add_replace.corp_id
    where tmp_merchant_add_replace.mcht_cd is null
    union all
    select
        main_t.merchant_id,
        main_t.p_date as act_start,
        99990101 as act_end,
        main_t.dt_type,
        main_t.mcht_cd,
        main_t.bran_cd,
        main_t.mcht_name,
        main_t.mcht_business_name,
        main_t.mcht_org_cd,
        main_t.mcht_business_license,
        main_t.mcht_type,
        main_t.mcht_service_type,
        main_t.servcie_org,
        main_t.reg_prov_cd,
        main_t.reg_city_cd,
        main_t.reg_country_cd,
        main_t.mcht_address,
        main_t.contact_name,
        main_t.contact_phone,
        main_t.contact_fax,
        main_t.contact_email,
        main_t.bloc_mcht_cd,
        main_t.bloc_mcht_name,
        main_t.mcht_settlement_way,
        main_t.approve_date,
        main_t.lose_date,
        main_t.service_level,
        main_t.source_from,
        main_t.referrer,
        main_t.customer_cd,
        main_t.customer_name,
        main_t.mcc_cd,
        main_t.org_cd,
        main_t.legal_person_name,
        main_t.legal_person_id_type,
        main_t.legal_person_id_no,
        main_t.mcht_settlement_cycle,
        main_t.normal_clt_count,
        main_t.undo_clt_count,
        main_t.mcht_nature,
        main_t.mcht_status,
        main_t.is_risk_lp,
        main_t.mcht_dept,
        main_t.has_overdue,
        main_t.financial_name,
        main_t.financial_phone,
        main_t.tax_reg_no,
        main_t.settlement_model,
        main_t.settlement_cycle,
        main_t.settlement_way,
        main_t.data_channel,
        main_t.is_group_mcht,
        main_t.is_point_card_settlement,
        main_t.customer_no,
        main_t.area_standard,
        main_t.reg_capital,
        main_t.main_business,
        main_t.scope_of_business,
        main_t.business_license_limit,
        main_t.service_nature,
        main_t.product_info,
        main_t.mcht_mcc,
        main_t.three_in_one_flag,
        main_t.us_credit_code_no,
        main_t.us_credit_code_start_date,
        main_t.us_credit_code_end_date,
        main_t.corp_id
    from
        dim.tmp_merchant_add_replace as main_t
) ddd
distribute by mcht_cd
sort by merchant_id
;




set hive.exec.compress.output=false;
set hive.merge.mapredfiles = true;




insert into table dim.dim_merchant_info_history
    partition (act_end=${hivevar:THE_DATE})
select
    main_t.merchant_id,
    main_t.corp_id,
    main_t.act_start,

    main_t.dt_type,
    main_t.mcht_cd,
    main_t.bran_cd,
    main_t.mcht_name,
    main_t.mcht_business_name,
    main_t.mcht_org_cd,
    main_t.mcht_business_license,
    main_t.mcht_type,
    main_t.mcht_service_type,
    main_t.servcie_org,
    main_t.reg_prov_cd,
    main_t.reg_city_cd,
    main_t.reg_country_cd,
    main_t.mcht_address,
    main_t.contact_name,
    main_t.contact_phone,
    main_t.contact_fax,
    main_t.contact_email,
    main_t.bloc_mcht_cd,
    main_t.bloc_mcht_name,
    main_t.mcht_settlement_way,
    main_t.approve_date,
    main_t.lose_date,
    main_t.service_level,
    main_t.source_from,
    main_t.referrer,
    main_t.customer_cd,
    main_t.customer_name,
    main_t.mcc_cd,
    main_t.org_cd,
    main_t.legal_person_name,
    main_t.legal_person_id_type,
    main_t.legal_person_id_no,
    main_t.mcht_settlement_cycle,
    main_t.normal_clt_count,
    main_t.undo_clt_count,
    main_t.mcht_nature,
    main_t.mcht_status,
    main_t.is_risk_lp,
    main_t.mcht_dept,
    main_t.has_overdue,
    main_t.financial_name,
    main_t.financial_phone,
    main_t.tax_reg_no,
    main_t.settlement_model,
    main_t.settlement_cycle,
    main_t.settlement_way,
    main_t.data_channel,
    main_t.is_group_mcht,
    main_t.is_point_card_settlement,
    main_t.customer_no,
    main_t.area_standard,
    main_t.reg_capital,
    main_t.main_business,
    main_t.scope_of_business,
    main_t.business_license_limit,
    main_t.service_nature,
    main_t.product_info,
    main_t.mcht_mcc,
    main_t.three_in_one_flag,
    main_t.us_credit_code_no,
    main_t.us_credit_code_start_date,
    main_t.us_credit_code_end_date
from dim.dim_merchant_info as main_t
LEFT SEMI JOIN dim.tmp_merchant_update
on
    main_t.mcht_cd = tmp_merchant_update.mcht_cd
    and main_t.corp_id = tmp_merchant_update.corp_id
;
