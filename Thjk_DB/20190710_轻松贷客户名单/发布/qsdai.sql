
SELECT
    merchant_ap                         as `商户号`
    ,aaa.inst_name                      as `机构名称`
    ,merchant_info.bran_cd              as `机构代码`
    ,concat(
        LPAD( merchant_info.reg_prov_cd, 2, '0'),
        LPAD( merchant_info.reg_city_cd, 2, '0'),
        LPAD( merchant_info.reg_country_cd, 2, '0')
    )                                   as `地区码`
    ,dw_area.area_name                  as `地区名称`
    ,merchant_info.reg_prov_cd          as `省份码`
    ,merchant_info.reg_city_cd          as `城市码`
    ,merchant_info.reg_country_cd       as `区县码`
    ,merchant_info.legal_person_name    as `法人姓名`

    ,merchant_info.mcht_name            as `商户名称`
    ,merchant_info.contact_phone        as `联系电话`
    ,fff.bl_login_mobile                as `好老板登录手机`
    ,fff.bl_bank_mobile_phone           as `好老板银行手机`
    ,fff.zc_login_mobile                as `通联宝登录手机`
    ,fff.zc_bank_mobile_phone           as `通联宝银行手机`

from ods_ftp.qsd_merchant
LEFT JOIN ods_ftp.merchant_info
on qsd_merchant.merchant_ap=merchant_info.mcht_cd
LEFT JOIN ods_ftp_opt.dw_area
on concat(
        LPAD( merchant_info.reg_prov_cd, 2, '0'),
        LPAD( merchant_info.reg_city_cd, 2, '0'),
        LPAD( merchant_info.reg_country_cd, 2, '0')
    ) = dw_area.area_no
LEFT JOIN
(
    SELECT inst_name,bran_cd
    from
    rds_rc.organization
    where bran_cd LIKE '99%'
        and inst_name like '%分公司'
) aaa
on merchant_info.bran_cd = aaa.bran_cd
left join
(
    SELECT
        coalesce(bbb.legal_id_no,ddd.legal_id_no) as legal_id_no,
        bbb.login_mobile as bl_login_mobile,
        bbb.bank_mobile_phone as bl_bank_mobile_phone,
        ddd.login_mobile as zc_login_mobile,
        ddd.bank_mobile_phone as zc_bank_mobile_phone
    from
    (
        SELECT
            login_mobile
            ,bank_mobile_phone
            ,id_no as legal_id_no
        from
        (
            SELECT
                login_mobile,bank_mobile_phone
                ,upper(regexp_replace(legal_id_no,'\\s',''))
                as id_no
                ,( row_number() over (PARTITION BY user.legal_id_no ORDER BY user.last_update_time desc ) )
                as row_num
            from rds_rc.`user`
            where source=1
        ) aaa
        where row_num = 1 and length(id_no)>=18
    ) bbb
    full join
    (
        SELECT
            login_mobile
            ,bank_mobile_phone
            ,id_no as legal_id_no
        from
        (
            SELECT
                login_mobile,bank_mobile_phone
                ,upper(regexp_replace(legal_id_no,'\\s',''))
                as id_no
                ,( row_number() over (PARTITION BY user.legal_id_no ORDER BY user.last_update_time desc ) )
                as row_num
            from rds_rc.`user`
            where source=2
        ) ccc
        where row_num = 1 and length(id_no)>=18
    ) ddd
    on bbb.legal_id_no = ddd.legal_id_no
 ) fff
 on upper(regexp_replace(merchant_info.legal_person_id_no,'\\s','')) = fff.legal_id_no
order by
    `机构名称` desc
    ,`地区名称`
    ,`法人姓名`
