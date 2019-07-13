-- CREATE TABLE `ods_ftp_opt`.`dw_merchant` (
CREATE TABLE `ods_ftp_opt`.`dw_merchant_tmp` (
  `t_id` int COMMENT '记录编号', -- HIVE建表
  `mcht_cd`  string COMMENT '商户编码',
  `corp_id`  string COMMENT '企业代码'
)
 ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\"",
    "escapeChar" = "\\"
)
STORED AS TEXTFILE;



select mcht_cd from ods_ftp.merchant_info limit 10;


insert into table `ods_ftp_opt`.`dw_merchant`
select row_number() OVER() as t_id, mcht_cd, '100001' as `corp_id`
from ods_ftp.merchant_info limit 1000000;

select * from  `ods_ftp_opt`.`dw_merchant` where t_id > 999959 or t_id < 50 order by cast(t_id as int);

/*
"$(cat <<EOF
xxx
EOF
)"
*/

-- xxx 里面，不能有 \t  不能有 `, 可以有'

/*
hive -e "$(cat <<EOF
sql
EOF)"
*/
set mapreduce.map.memory.mb=4096; -- 只要大于1024，hive默认分配的内存分大一倍，也就是2048M
set mapreduce.reduce.memory.mb=4096;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 4000;
SET hive.exec.max.dynamic.partitions=4000;


-- ===设置map输出和reduce输出进行合并的相关参数：
-- #设置map端输出进行合并，默认为true
set hive.merge.mapfiles = true;

-- #设置reduce端输出进行合并，默认为false
set hive.merge.mapredfiles = true;

-- #设置合并文件的大小
set hive.merge.size.per.task = 256000000;

-- #当输出文件的平均大小小于该值时，启动一个独立的MapReduce任务进行文件merge。
set hive.merge.smallfiles.avgsize=16000000;


insert into table ods_ftp_opt.dw_merchant_tmp
select
   t_id,mcht_cd,corp_id
from
   ods_ftp_opt.dw_merchant
union all
select
    cast( ( row_number() OVER() ) + ttt2.t_id_max as int ) as t_id, ttt1.mcht_cd, '100001' as corp_id
from
(
    select
        merchant_info.mcht_cd
    from
        ods_ftp.merchant_info
    left join
        ods_ftp_opt.dw_merchant
    on merchant_info.mcht_cd=dw_merchant.mcht_cd
    where dw_merchant.mcht_cd is null
) ttt1
left join
(
    select
        coalesce(max(cast(t_id as int)),0)    t_id_max
    from ods_ftp_opt.dw_merchant
) ttt2
;




/*
第一次

1       821650159980160 100001
2       821620550390002 100001
3       821611060510197 100001
4       821610170110245 100001
5       821610159981930 100001
6       821610151371555 100001
7       821610151371427 100001
8       821532559980034 100001
9       821532379110003 100001
10      821530170110106 100001
999983  55145108220N3L7 100001
999984  55145107399N1MD 100001
999985  55142405331N44P 100001
999986  55142305331N4X4 100001
999987  55142108299N2MA 100001
999988  55142105499N5CQ 100001
999989  55139107399N5TZ 100001
999990  55131805039N3DV 100001
999991  55126105399N3NB 100001
999992  55124105948N588 100001
999993  55124105948N2JP 100001
999994  55124105912N2JF 100001
999995  55120101520N34V 100001
999996  55119200780N5FH 100001
999997  55119105039N1NM 100001
999998  55188104812N3QX 100001
999999  55182105311N2KS 100001
1000000 55179501520N4EB 100001

*/




SELECT
    merchant_ap     -- 商户号
    ,aaa.inst_name  -- 商户名称
    ,merchant_info.bran_cd  -- 所属机构
    ,concat(
        if(length(merchant_info.reg_prov_cd)=2,merchant_info.reg_prov_cd,'00'),
        if(length(merchant_info.reg_city_cd)=2,merchant_info.reg_city_cd,'00'),
        if(length(merchant_info.reg_country_cd)=2,merchant_info.reg_country_cd,'00')
    ) as arec_code      -- 合成地区码
    ,dw_area.area_name  -- 地区名称
    ,merchant_info.reg_prov_cd          -- 省份码
    ,merchant_info.reg_city_cd          -- 城市码
    ,merchant_info.reg_country_cd       -- 区县码
    ,merchant_info.legal_person_name    -- 法人姓名
    ,upper(regexp_replace(merchant_info.legal_person_id_no,'\\s','')) as id_no  -- 身份证号码去空格大写
    ,merchant_info.mcht_name            -- 商户名称
    ,merchant_info.contact_phone        -- 联系电话
    ,fff.bl_login_mobile                -- 保理登录手机
    ,fff.bl_bank_mobile_phone           -- 保理银行手机
    ,fff.zc_login_mobile                -- 资产登录手机
    ,fff.zc_bank_mobile_phone           -- 资产银行手机
    --  ,merchant_info.*
from ods_ftp.qsd_merchant               -- 商户号，年龄范围，流水额度
LEFT JOIN ods_ftp.merchant_info         -- 商户基础信息表
on qsd_merchant.merchant_ap=merchant_info.mcht_cd       -- 商户号相等
LEFT JOIN ods_ftp_opt.dw_area           -- 地区表
on concat(
        if(length(merchant_info.reg_prov_cd)=2,merchant_info.reg_prov_cd,'00'),
        if(length(merchant_info.reg_city_cd)=2,merchant_info.reg_city_cd,'00'),
        if(length(merchant_info.reg_country_cd)=2,merchant_info.reg_country_cd,'00')
    ) = dw_area.area_no                 -- 地区编码一致
LEFT JOIN
(                               -- 机构名称表
    SELECT inst_name,bran_cd            -- 机构名称，机构编号
    from
    rds_rc.organization                 -- 机构表
    where bran_cd LIKE '99%'            -- 99 开头才是分公司
) aaa
on merchant_info.bran_cd = aaa.bran_cd  -- 机构编号相等
left join
(                               -- 资产保理注册用户手机号码
    SELECT
        coalesce(bbb.legal_id_no,ddd.legal_id_no) as legal_id_no,   -- 保理资产有一个有数据就可以
        bbb.login_mobile as bl_login_mobile,                        -- 保理登录手机
        bbb.bank_mobile_phone as bl_bank_mobile_phone,              -- 保理银行手机
        ddd.login_mobile as zc_login_mobile,                        -- 资产登录手机
        ddd.bank_mobile_phone as zc_bank_mobile_phone               -- 资产银行手机
    from
    (                           -- 保理手机号码表
        SELECT
            login_mobile                                            -- 登录手机号
            ,bank_mobile_phone                                      -- 银行手机号
            ,id_no as legal_id_no                                   -- 法人身份证
        from
        (
            SELECT
                login_mobile,bank_mobile_phone
                ,upper(regexp_replace(legal_id_no,'\\s',''))        -- 身份证号码去空格大写
                as id_no
                ,( row_number() over (PARTITION BY user.legal_id_no ORDER BY user.last_update_time desc ) )
                as row_num                                          -- 有多个记录，只要最新的一个
            from rds_rc.`user`
            where source=1                                          -- 保理系统
        ) aaa
        where row_num = 1 and length(id_no)>=18                     -- 有多个记录，只要最新的一个
    ) bbb
    full join
    (                           -- 资产手机号码表
        SELECT
            login_mobile                                            -- 登录手机号
            ,bank_mobile_phone                                      -- 银行手机号
            ,id_no as legal_id_no                                   -- 法人身份证
        from
        (
            SELECT
                login_mobile,bank_mobile_phone
                ,upper(regexp_replace(legal_id_no,'\\s',''))        -- 身份证号码去空格大写
                as id_no
                ,( row_number() over (PARTITION BY user.legal_id_no ORDER BY user.last_update_time desc ) )
                as row_num                                          -- 有多个记录，只要最新的一个
            from rds_rc.`user`
            where source=2                                          -- 资产系统
        ) ccc
        where row_num = 1 and length(id_no)>=18                     -- 有多个记录，只要最新的一个
    ) ddd
    on bbb.legal_id_no = ddd.legal_id_no                            -- 用身份证号码关联
 ) fff
 on upper(regexp_replace(merchant_info.legal_person_id_no,'\\s','')) = fff.legal_id_no  -- 用身份证号码关联
;

