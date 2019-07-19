-- 创建客户经理代码表
CREATE TABLE IF NOT EXISTS `ods_ftp_opt`.`dim_code_manager` (
    `platform_code` string COMMENT '平台代码',
    `manager_code`  string COMMENT '客户经理代码',
    `cert_no`  string COMMENT '身份证',
    `name`  string COMMENT '姓名',
    `phone`  string COMMENT '电话',
    `email`  string COMMENT '邮箱',
    `f_platform_code`  string COMMENT '父账号平台代码',
    `f_manager_code` int COMMENT '父账号客户经理代码',
    `manager_id` int COMMENT '身份证表ID'
)
 ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\"",
    "escapeChar" = "\\"
)
STORED AS TEXTFILE;


-- ===========================================================

DROP TABLE IF EXISTS `ods_ftp_opt`.`dim_code_manager_tmp`;
CREATE TABLE  IF NOT EXISTS `ods_ftp_opt`.`dim_code_manager_tmp` (
    `platform_code` string COMMENT '平台代码',
    `manager_code`  string COMMENT '客户经理代码',
    `cert_no`  string COMMENT '身份证',
    `name`  string COMMENT '姓名',
    `phone`  string COMMENT '电话',
    `email`  string COMMENT '邮箱',
    `f_platform_code`  string COMMENT '父账号平台代码',
    `f_manager_code` int COMMENT '父账号客户经理代码',
    `manager_id` int COMMENT '身份证表ID'
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\"",
    "escapeChar" = "\\"
)
STORED AS TEXTFILE;

-- 创建客户经理临时代码表
CREATE TEMPORARY TABLE  `ods_ftp_opt`.`tmp_eee` (
    `platform_code` string COMMENT '平台代码',
    `manager_code`  string COMMENT '客户经理代码',
    `cert_no`  string COMMENT '身份证',
    `name`  string COMMENT '姓名',
    `phone`  string COMMENT '电话',
    `email`  string COMMENT '邮箱',
    `manager_id` int COMMENT '身份证表ID'
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\"",
    "escapeChar" = "\\"
)
STORED AS TEXTFILE;


CREATE TEMPORARY TABLE  `ods_ftp_opt`.`tmp_ggg` (
    `s_platform_code` string COMMENT '平台代码',
    `s_manager_code`  string COMMENT '客户经理代码',
    `s_cert_no`  string COMMENT '身份证',
    `s_name`  string COMMENT '姓名',
    `s_phone`  string COMMENT '电话',
    `s_email`  string COMMENT '邮箱',
    `s_manager_id` int COMMENT '身份证表ID',

    `platform_code` string COMMENT '平台代码',
    `manager_code`  string COMMENT '客户经理代码',
    `cert_no`  string COMMENT '身份证',
    `name`  string COMMENT '姓名',
    `phone`  string COMMENT '电话',
    `email`  string COMMENT '邮箱',
    `manager_id` int COMMENT '身份证表ID'
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\"",
    "escapeChar" = "\\"
)
STORED AS TEXTFILE;

with aaa as -- BEGIN:  最新客户经理融表，清洗空格和NULL
(
    SELECT
        2 as platform_code
        , lower(regexp_replace(coalesce(email,'null'),'\\s','')) manager_code
        , upper(regexp_replace(coalesce(cert_no,'NULL'),'\\s','')) cert_no
        , lower(regexp_replace(coalesce(name,'null'),'\\s','')) name
        , lower(regexp_replace(coalesce(phone,'null'),'\\s','')) phone
        , lower(regexp_replace(coalesce(user_email,'null'),'\\s','')) email
        , 0 as manager_id
    from rds_rc.t_operation_emp
    where t_operation_emp.`ARCHIVE_FLAG` = 1 and t_operation_emp.`STATUS` in ('1','2','9')
), -- END:  最新客户经理融表，清洗空格和NULL
bbb as -- BEGIN:  历史客户经理融表，清洗空格和NULL
(
    SELECT
        lower(regexp_replace(coalesce(dim_code_manager.platform_code,'null'),'\\s','')) platform_code
        , lower(regexp_replace(coalesce(dim_code_manager.manager_code,'null'),'\\s','')) manager_code
        , upper(regexp_replace(coalesce(dim_code_manager.cert_no,'NULL'),'\\s','')) cert_no
        , lower(regexp_replace(coalesce(dim_code_manager.name,'null'),'\\s','')) name
        , lower(regexp_replace(coalesce(dim_code_manager.phone,'null'),'\\s','')) phone
        , lower(regexp_replace(coalesce(dim_code_manager.email,'null'),'\\s','')) email
        , coalesce(dim_code_manager.manager_id,0) manager_id
    from `ods_ftp_opt`.`dim_code_manager`
) -- END:  历史客户经理融表，清洗空格和NULL
insert into table ods_ftp_opt.tmp_eee
  -- BEGIN: 最新和历史客户经理融合
    SELECT
        bbb.platform_code
        , bbb.manager_code
        , bbb.cert_no
        , bbb.name
        , bbb.phone
        , bbb.email
        , bbb.manager_id
    from bbb
    left join aaa
    on
        bbb.platform_code = aaa.platform_code
        and bbb.manager_code = aaa.manager_code
    where
        aaa.manager_code is null and aaa.platform_code is null

    union all

    SELECT
        aaa.platform_code
        , aaa.manager_code
        , aaa.cert_no
        , aaa.name
        , aaa.phone
        , aaa.email
        , coalesce(bbb.manager_id, 0) manager_id
    from aaa
    left join bbb
    on
        bbb.platform_code = aaa.platform_code
        and bbb.manager_code = aaa.manager_code
 -- END: 最新和历史客户经理融合
;


with eee as (
    select
        platform_code
        , manager_code
        , cert_no
        , name
        , phone
        , email
        , manager_id
    from ods_ftp_opt.tmp_eee
)
insert into table ods_ftp_opt.tmp_ggg
-- ggg as -- BEGIN 最新和历史客户经理融合后，计算出可能是一个人名下的多个客户经理编码
    select
        eee.platform_code s_platform_code
        , if(eee.manager_code<fff.manager_code,eee.manager_code,fff.manager_code) s_manager_code
        , if(length(eee.cert_no)>length(fff.cert_no),eee.cert_no,fff.cert_no) s_cert_no
        , if(length(eee.name)>length(fff.name),eee.name,fff.name) s_name
        , if(length(eee.phone)>length(fff.phone),eee.phone,fff.phone) s_phone
        , if(length(eee.email)>length(fff.email),eee.email,fff.email) s_email
        , if(eee.manager_id>fff.manager_id,eee.manager_id,coalesce(fff.manager_id,eee.manager_id)) s_manager_id
        , eee.*
    from
    eee
    inner join
    eee fff
    on
        -- 一. 姓名相同、身份证相同
        eee.platform_code = fff.platform_code
        
        -- 1、姓名相同
        and eee.name = fff.name
        and length(eee.name) >= 2 and eee.name <> 'null'
        
        -- 2、身份证相同    
        and eee.cert_no = fff.cert_no
        and length(eee.cert_no) >= 5  -- 代表  身份证 is not null
    where
        -- 0、不能是自己和自己交叉
        eee.manager_code <> fff.manager_code  -- 也代表  fff.manager_code is not null

    union all

    select
        eee.platform_code s_platform_code
        ,if(eee.manager_code<fff.manager_code,eee.manager_code,fff.manager_code) s_manager_code
        , if(length(eee.cert_no)>length(fff.cert_no),eee.cert_no,fff.cert_no) s_cert_no
        , if(length(eee.name)>length(fff.name),eee.name,fff.name) s_name
        , if(length(eee.phone)>length(fff.phone),eee.phone,fff.phone) s_phone
        , if(length(eee.email)>length(fff.email),eee.email,fff.email) s_email
        , if(eee.manager_id>fff.manager_id,eee.manager_id,coalesce(fff.manager_id,eee.manager_id)) s_manager_id
        , eee.*
    from
    eee
    inner join
    eee fff
    on
        -- 二. eee.name=fff.name, and eee.iden和fff.iden至少一个为空,  and fff.email=fff.email
        eee.platform_code = fff.platform_code

        -- 1、姓名相同
        and eee.name = fff.name
        and length(eee.name) >= 2 and eee.name <> 'null'
        
        -- 2、email 相同
        and eee.email = fff.email
        and length(eee.email) >= 5  -- 代表  eee.email is not null
    where
        -- 0、不能是自己和自己交叉
        eee.manager_code <> fff.manager_code  -- 也代表  fff.manager_code is not null
        and -- 3、身份证至少一个为空
        (     eee.cert_no = 'NULL' or fff.cert_no = 'NULL'
        )
         
    union all
        
    select
        eee.platform_code s_platform_code
        , if(eee.manager_code<fff.manager_code,eee.manager_code,fff.manager_code) s_manager_code
        , if(length(eee.cert_no)>length(fff.cert_no),eee.cert_no,fff.cert_no) s_cert_no
        , if(length(eee.name)>length(fff.name),eee.name,fff.name) s_name
        , if(length(eee.phone)>length(fff.phone),eee.phone,fff.phone) s_phone
        , if(length(eee.email)>length(fff.email),eee.email,fff.email) s_email
        , if(eee.manager_id>fff.manager_id,eee.manager_id,coalesce(fff.manager_id,eee.manager_id)) s_manager_id
        , eee.*
    from
    eee
    inner join
    eee fff
    on
        -- 三A. eee.name=fff.name,   eee.iden至少一个为空 , email<>email ,  eee.manager_code=fff.phone
        eee.platform_code = fff.platform_code

        -- 1、姓名相同
        and eee.name = fff.name
        and length(eee.name) >= 2 and eee.name <> 'null'
        
        -- 2、eee.manager_code 和 fff.phone 相同
        and eee.manager_code = fff.phone
        and length(eee.manager_code) >= 5  -- 代表  eee.manager_code is not null
    where
        -- 0、不能是自己和自己交叉
        eee.manager_code <> fff.manager_code  -- 也代表  fff.code is not null
        and -- 3、身份证至少一个为空
        (     eee.cert_no = 'NULL' or fff.cert_no = 'NULL'
        )
        and    -- 4、user_email 不相等，或者至少一个为空
        (    eee.email <> fff.email
         or eee.email = 'null'
         or fff.email = 'null'
        )
        
    union all
        
    select
        eee.platform_code s_platform_code
        , if(eee.manager_code<fff.manager_code,eee.manager_code,fff.manager_code) s_manager_code
        , if(length(eee.cert_no)>length(fff.cert_no),eee.cert_no,fff.cert_no) s_cert_no
        , if(length(eee.name)>length(fff.name),eee.name,fff.name) s_name
        , if(length(eee.phone)>length(fff.phone),eee.phone,fff.phone) s_phone
        , if(length(eee.email)>length(fff.email),eee.email,fff.email) s_email
        , if(eee.manager_id>fff.manager_id,eee.manager_id,coalesce(fff.manager_id,eee.manager_id)) s_manager_id
        , eee.*
    from
    eee
    inner join
    eee fff
    on
        -- 三B. eee.name=fff.name,   eee.iden至少一个为空 , email<>email ,  eee.manager_code=fff.phone
        eee.platform_code = fff.platform_code

        -- 1、姓名相同
        and eee.name = fff.name
        and length(eee.name) >= 2 and eee.name <> 'null'
        
        -- 2、eee.code 和 fff.phone 相同
        and fff.manager_code = eee.phone
        and length(fff.manager_code) >= 5  -- 代表  eee.manager_code is not null
    where
        -- 0、不能是自己和自己交叉
        eee.manager_code <> fff.manager_code  -- 也代表  fff.manager_code is not null
        and -- 3、身份证至少一个为空
        (     eee.cert_no = 'NULL' or fff.cert_no = 'NULL'
        )
        and    -- 4、user_email 不相等，或者至少一个为空
        (    eee.email <> fff.email
         or eee.email = 'null'
         or fff.email = 'null'
        )
;--) -- END:  最新和历史客户经理融合后，计算出可能是一个人名下的多个客户经理编码


add jar MaxStrUDAF.jar;


create temporary function maxstr as 'com.wind.hive.MaxStrUDAF';


insert into table ods_ftp_opt.dim_code_manager_tmp
select
    ggg.platform_code
    , ggg.manager_code
    , maxstr(ggg.cert_no) cert_no
    , maxstr(ggg.name) name
    , maxstr(ggg.phone) phone
    , maxstr(ggg.email) email
    , min(ggg.s_platform_code) f_platform_code
    , min(ggg.s_manager_code) f_manager_code
    -- , max(ggg.s_cert_no) f_cert_no  -- 应该取最长的
    -- , max(ggg.s_name) f_name        -- 应该取最长的
    -- , max(ggg.s_phone) f_phone      -- 应该取最长的
    -- , max(ggg.s_email) f_email    -- 应该取最长的
    , max(ggg.manager_id) manager_id
from ods_ftp_opt.tmp_ggg ggg
group by ggg.platform_code, ggg.manager_code
order by f_platform_code, f_manager_code
union all
select
    eee.platform_code
    , eee.manager_code
    , eee.cert_no
    , eee.name
    , eee.phone
    , eee.email
    , eee.platform_code f_platform_code
    , eee.manager_code f_manager_code    
    , eee.manager_id
from ods_ftp_opt.tmp_eee eee
left join ods_ftp_opt.tmp_ggg ggg
on
    eee.platform_code = ggg.platform_code
    and eee.manager_code = ggg.manager_code
where
    ggg.platform_code is null and ggg.manager_code is null                          -- 不在多用户集合里
order by f_platform_code, f_manager_code
;
-- 6582     6538 + 44
DROP TABLE IF EXISTS `ods_ftp_opt`.`dim_code_manager`;
ALTER TABLE `ods_ftp_opt`.`dim_code_manager_tmp` RENAME TO `ods_ftp_opt`.`dim_code_manager`;
