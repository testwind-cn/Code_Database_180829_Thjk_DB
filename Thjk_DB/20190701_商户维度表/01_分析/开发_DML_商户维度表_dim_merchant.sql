

drop table if exists  dim.tmp_dim_merchant;

-- CREATE TABLE dim.dim_merchant (
CREATE TABLE if not exists dim.tmp_dim_merchant (
    merchant_id int COMMENT '记录编号', -- HIVE建表
    mcht_cd  string COMMENT '商户编码',
    reg_prov_cd  string COMMENT '省编码',
    reg_city_cd  string COMMENT '市编码',
    reg_country_cd  string COMMENT '区县编码',
    mcht_type  string COMMENT '商户类型',
    bloc_mcht_cd  string COMMENT '集团编码',
    legal_person_id_no  string COMMENT '法人身份证',
    mcht_business_license  string COMMENT '营业执照'
)
PARTITIONED BY ( corp_id  string COMMENT '企业代码')
CLUSTERED BY (mcht_cd) INTO 20 BUCKETS
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\"",
    "escapeChar" = "\\"
)
STORED AS TEXTFILE;






truncate table dim.tmp_dim_merchant;
-- 默认 hive.enforce.bucketing=true
-- SET hive.enforce.bucketing = false;
SET hive.enforce.bucketing = false;  -- ####### 开启 ###########
set hive.exec.compress.output=false;

-- -----
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


-- 在hadoop2中有些参数名称过时了，例如原来的mapred.reduce.tasks改名为mapreduce.job.reduces了，当然，这两个参数你都可以使用，只是第一个参数过时了。
-- set mapred.reduce.tasks=20;

-- In order to set a constant number of reducers:
set mapreduce.job.reduces=20;



SET hive.enforce.bucketing = false;


insert into table dim.tmp_dim_merchant
    partition (corp_id='10001')
select
    row_number() OVER() as merchant_id
    ,mcht_cd
    ,reg_prov_cd
    ,reg_city_cd
    ,reg_country_cd
    ,mcht_type
    ,bloc_mcht_cd
    ,legal_person_id_no
    ,mcht_business_license
from dim.dim_merchant_info_20190122
DISTRIBUTE BY mcht_cd
SORT BY merchant_id
;





-- SELECT count( DISTINCT mcht_cd ) from dw_2g.dwd_merchant_info_20190122 ;
-- 7713454


drop  table if exists dim.dim_merchant ;

alter  table  dim.tmp_dim_merchant rename to dim.dim_merchant;



select * from  dim.dim_merchant where merchant_id > 999959 or merchant_id < 50 order by cast(merchant_id as int);


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

-- 之前有 7713454 个商户号


insert into table dim.tmp_dim_merchant
    partition (corp_id)
select
    merchant_id
    , mcht_cd
    , reg_prov_cd, reg_city_cd, reg_country_cd
    , mcht_type, bloc_mcht_cd
    , legal_person_id_no, mcht_business_license
    , corp_id
from (
    select
        cast(merchant_id as int) as merchant_id
        , mcht_cd
        , reg_prov_cd, reg_city_cd, reg_country_cd
        , mcht_type, bloc_mcht_cd
        , legal_person_id_no, mcht_business_license
        , corp_id
    from
       dim.dim_merchant
    union all
    select
        cast( ttt1.add_merchant_id + ttt2.max_id as bigint ) as merchant_id
        , ttt1.mcht_cd
        , ttt1.reg_prov_cd, ttt1.reg_city_cd, ttt1.reg_country_cd
        , ttt1.mcht_type, ttt1.bloc_mcht_cd
        , ttt1.legal_person_id_no, ttt1.mcht_business_license
        , ttt1.corp_id
    from
    (
        select
            row_number() OVER() as add_merchant_id
            , dim_merchant_info.mcht_cd
            , dim_merchant_info.reg_prov_cd, dim_merchant_info.reg_city_cd, dim_merchant_info.reg_country_cd
            , dim_merchant_info.mcht_type, dim_merchant_info.bloc_mcht_cd
            , dim_merchant_info.legal_person_id_no, dim_merchant_info.mcht_business_license
            , dim_merchant_info.corp_id
        from
            dim.dim_merchant_info
        left join
            dim.dim_merchant
        on dim_merchant_info.mcht_cd=dim_merchant.mcht_cd
        where dim_merchant.mcht_cd is null
    ) ttt1
    left join
    (
        select
            coalesce(max(cast(merchant_id as int)),0) as max_id
        from dim.dim_merchant
    ) ttt2
) aaa
DISTRIBUTE BY mcht_cd
SORT BY merchant_id
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



