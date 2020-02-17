













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





insert into table dim.dim_merchant_info
    partition (corp_id)
select
    row_number() OVER() as merchant_id,
    20150101 as act_start,
    99990101 as act_end,
    'NEW' as dt_type,
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
    case upper(coalesce(tt_type,''))
        when 'MCHT' then 10001
        when 'SMCH' then 10002
        when '' then 10001
        else 10001
    end as corp_id
from dim.dim_merchant_info_20190122
DISTRIBUTE BY mcht_cd
SORT BY merchant_id
;

