
CREATE TABLE IF NOT EXISTS `dw_2g`.`dim_code_manager` (
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




DROP TABLE IF EXISTS `dw_2g`.`tmp_dim_code_manager`;
CREATE TABLE  IF NOT EXISTS `dw_2g`.`tmp_dim_code_manager` (
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


CREATE TEMPORARY TABLE  `dw_2g`.`tmp_eee` (
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


CREATE TEMPORARY TABLE  `dw_2g`.`tmp_ggg` (
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

with aaa as
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
),
bbb as
(
    SELECT
        lower(regexp_replace(coalesce(dim_code_manager.platform_code,'null'),'\\s','')) platform_code
        , lower(regexp_replace(coalesce(dim_code_manager.manager_code,'null'),'\\s','')) manager_code
        , upper(regexp_replace(coalesce(dim_code_manager.cert_no,'NULL'),'\\s','')) cert_no
        , lower(regexp_replace(coalesce(dim_code_manager.name,'null'),'\\s','')) name
        , lower(regexp_replace(coalesce(dim_code_manager.phone,'null'),'\\s','')) phone
        , lower(regexp_replace(coalesce(dim_code_manager.email,'null'),'\\s','')) email
        , coalesce(dim_code_manager.manager_id,0) manager_id
    from `dw_2g`.`dim_code_manager`
)
insert into table dw_2g.tmp_eee

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
    from dw_2g.tmp_eee
)
insert into table dw_2g.tmp_ggg

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

        eee.platform_code = fff.platform_code
        

        and eee.name = fff.name
        and length(eee.name) >= 2 and eee.name <> 'null'
        

        and eee.cert_no = fff.cert_no
        and length(eee.cert_no) >= 5
    where

        eee.manager_code <> fff.manager_code

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

        eee.platform_code = fff.platform_code


        and eee.name = fff.name
        and length(eee.name) >= 2 and eee.name <> 'null'
        

        and eee.email = fff.email
        and length(eee.email) >= 5
    where

        eee.manager_code <> fff.manager_code
        and
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

        eee.platform_code = fff.platform_code


        and eee.name = fff.name
        and length(eee.name) >= 2 and eee.name <> 'null'
        

        and eee.manager_code = fff.phone
        and length(eee.manager_code) >= 5
    where

        eee.manager_code <> fff.manager_code
        and
        (     eee.cert_no = 'NULL' or fff.cert_no = 'NULL'
        )
        and
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

        eee.platform_code = fff.platform_code


        and eee.name = fff.name
        and length(eee.name) >= 2 and eee.name <> 'null'
        

        and fff.manager_code = eee.phone
        and length(fff.manager_code) >= 5
    where

        eee.manager_code <> fff.manager_code
        and
        (     eee.cert_no = 'NULL' or fff.cert_no = 'NULL'
        )
        and
        (    eee.email <> fff.email
         or eee.email = 'null'
         or fff.email = 'null'
        )
;


add jar MaxStrUDAF.jar;


create temporary function maxstr as 'com.wind.hive.MaxStrUDAF';


insert into table dw_2g.tmp_dim_code_manager
select
    ggg.platform_code
    , ggg.manager_code
    , maxstr(ggg.cert_no) cert_no
    , maxstr(ggg.name) name
    , maxstr(ggg.phone) phone
    , maxstr(ggg.email) email
    , min(ggg.s_platform_code) f_platform_code
    , min(ggg.s_manager_code) f_manager_code




    , max(ggg.manager_id) manager_id
from dw_2g.tmp_ggg ggg
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
from dw_2g.tmp_eee eee
left join dw_2g.tmp_ggg ggg
on
    eee.platform_code = ggg.platform_code
    and eee.manager_code = ggg.manager_code
where
    ggg.platform_code is null and ggg.manager_code is null
order by f_platform_code, f_manager_code
;

