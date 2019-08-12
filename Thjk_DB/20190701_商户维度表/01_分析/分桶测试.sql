-- CVS 格式建表，字段类型按 INT，但实际还是 string


SELECT
    tt_type
    ,case upper(coalesce(tt_type,''))
        when 'MCHT' then 10001
        when 'SMCH' then 10002
        when '' then 10001
        else 10001
    end as ddd
    ,*
from dwd_merchant_info_20190122
TABLESAMPLE(BUCKET 30 OUT OF 15000 on mcht_cd )
where
    -- tt_type='MCHT'   1081997 个是 821
    -- tt_type='SMCH'   6146007 个是 收银宝
    tt_type is null or length(tt_type) = 0  -- 485450 个是 821
LIMIT 100;




set hive.exec.compress.output=false;
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=4096;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 4000;
SET hive.exec.max.dynamic.partitions=4000;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=16000000;


set mapreduce.job.reduces=20;
SET hive.enforce.bucketing = false;  -- ####### 开启 ###########


CREATE TABLE if not exists dw_2g.tmp_dim_merchant (
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

set mapreduce.job.reduces=20;
SET hive.enforce.bucketing = false;




-- =====================================================================================
-- |                                                                                   |
-- 第一种做法，先分桶，再使用行号做编号
-- 1、先按 mcht_cd 分桶，
-- 2、对分桶后的文件，取行号，是每个桶文件依次排号。
-- 3、再按 mcht_cd 分发到20个桶，（桶数没变，文件没有在桶间移动）
-- 4、各个桶内数据按 merchant_id 排序，需要转换成整数
-- 5、生成了 20 个文件
-- == 不使用第一种，使用第二种做法

truncate table dw_2g.tmp_dim_merchant;

insert into table dw_2g.tmp_dim_merchant
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
from (
    select
        mcht_cd
        ,reg_prov_cd
        ,reg_city_cd
        ,reg_country_cd
        ,mcht_type
        ,bloc_mcht_cd
        ,legal_person_id_no
        ,mcht_business_license
    from dw_2g.dwd_merchant_info_20190122
    DISTRIBUTE BY mcht_cd
)  aaa
DISTRIBUTE BY mcht_cd
-- SORT BY cast(merchant_id as int)     -- 和下面的SORT 效果一样，两种都可以正常排序
SORT BY merchant_id
;
-- 这个代码中，SORT 后面的 merchant_id 如果不转换成整数，不会按照字符串排序


SELECT * from (
    SELECT
        min(cast(merchant_id as bigint) ) as min_id
        ,max(cast(merchant_id as bigint) ) as max_id
        ,max(cast(merchant_id as bigint) ) -min(cast(merchant_id as bigint) ) as data_range
        ,count(*) as data_num
        ,INPUT__FILE__NAME
    from tmp_dim_merchant
    GROUP BY INPUT__FILE__NAME
) aaa
ORDER BY  aaa.min_id
;

/*
SORT BY cast(merchant_id as int)
aaa.min_id	aaa.max_id	aaa.data_range	aaa.data_num	aaa.input__file__name
    1	385143	385142	385143	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000007_0
385144	770609	385465	385466	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000019_0
770610	1156669	386059	386060	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000013_0
1156670	1542657	385987	385988	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000003_0
1542658	1929503	386845	386846	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000002_0
1929504	2315089	385585	385586	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000015_0
2315090	2700903	385813	385814	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000010_0
2700904	3086294	385390	385391	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000009_0
3086295	3472482	386187	386188	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000006_0
3472483	3858145	385662	385663	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000017_0
3858146	4242950	384804	384805	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000012_0
4242951	4629139	386188	386189	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000005_0
4629140	5014504	385364	385365	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000000_0
5014505	5399653	385148	385149	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000016_0
5399654	5786484	386830	386831	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000011_0
5786485	6171914	385429	385430	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000008_0
6171915	6556550	384635	384636	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000004_0
6556551	6942431	385880	385881	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000018_0
6942432	7327780	385348	385349	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000014_0
7327781	7713454	385673	385674	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000001_0
*/


/*
SORT BY cast(merchant_id as int)
    1	385586	385585	385586	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000015_0
385587	770729	385142	385143	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000007_0
770730	1156918	386188	386189	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000005_0
1156919	4244230	3087311	386831	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000011_0
1438002	1823815	385813	385814	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000010_0
1823816	2209206	385390	385391	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000009_0
2209207	2594636	385429	385430	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000008_0
2594637	2980624	385987	385988	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000003_0
2980625	3367470	386845	386846	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000002_0
3367471	3753133	385662	385663	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000017_0
3753134	4138482	385348	385349	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000014_0
4244231	4629696	385465	385466	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000019_0
4629697	5015577	385880	385881	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000018_0
5015578	5401765	386187	386188	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000006_0
5401766	5786401	384635	384636	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000004_0
5786402	6172075	385673	385674	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000001_0
6172076	6557224	385148	385149	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000016_0
6557225	6943284	386059	386060	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000013_0
6943285	7328089	384804	384805	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000012_0
7328090	7713454	385364	385365	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000000_0
*/

/*
SORT BY merchant_id
    1	385466	385465	385466	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000019_0
385467	3472126	3086659	385391	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000009_0
667355	1052784	385429	385430	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000008_0
1052785	1437927	385142	385143	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000007_0
1437928	1824115	386187	386188	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000006_0
1824116	2210304	386188	386189	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000005_0
2210305	2595978	385673	385674	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000001_0
2595979	2982809	386830	386831	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000011_0
2982810	3368623	385813	385814	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000010_0
3472127	6169478	2697351	385349	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000014_0
3756152	4142211	386059	386060	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000013_0
4142212	4527016	384804	384805	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000012_0
4527017	4911652	384635	384636	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000004_0
4911653	5297640	385987	385988	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000003_0
5297641	5683005	385364	385365	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000000_0
5683006	6068154	385148	385149	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000016_0
6169479	6555359	385880	385881	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000018_0
6555360	6942205	386845	386846	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000002_0
6942206	7327868	385662	385663	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000017_0
7327869	7713454	385585	385586	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000015_0
*/

/*
SORT BY merchant_id
    1	385143	385142	385143	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000007_0
385144	3085877	2700733	385663	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000017_0
666969	1052317	385348	385349	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000014_0
1052318	1439148	386830	386831	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000011_0
1439149	1824539	385390	385391	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000009_0
1824540	2209969	385429	385430	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000008_0
2209970	2596158	386188	386189	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000005_0
2596159	2982039	385880	385881	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000018_0
3085878	5785786	2699908	385814	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000010_0
3368049	3752684	384635	384636	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000004_0
3752685	4138672	385987	385988	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000003_0
4138673	4525518	386845	386846	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000002_0
4525519	4911192	385673	385674	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000001_0
4911193	5296557	385364	385365	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000000_0
5296558	5682143	385585	385586	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000015_0
5785787	6171252	385465	385466	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000019_0
6171253	6556401	385148	385149	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000016_0
6556402	6942461	386059	386060	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000013_0
6942462	7327266	384804	384805	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000012_0
7327267	7713454	386187	386188	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000006_0

*/


-- |                                                                                   |
-- =====================================================================================




-- =====================================================================================
-- |                                                                                   |
-- 第二种做法，先使用行号做编号，再分桶
-- 1、先取行号，是一个没有分桶的大文件
-- 2、再按 mcht_cd 分发到20个桶
-- 3、各个桶内数据按 merchant_id 排序，不需要转换成整数
-- 4、生成了 20 个文件

truncate table dw_2g.tmp_dim_merchant;

insert into table dw_2g.tmp_dim_merchant
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
from dw_2g.dwd_merchant_info_20190122
DISTRIBUTE BY mcht_cd
SORT BY merchant_id
;

/*
SORT BY merchant_id  两次执行结果序号都一样
aaa.min_id	aaa.max_id	aaa.data_range	aaa.data_num	aaa.input__file__name
1	7713445	7713444	385466	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000019_0
2	7713441	7713439	386189	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000005_0
3	7713452	7713449	386831	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000011_0
4	7713432	7713428	385149	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000016_0
6	7713447	7713441	385663	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000017_0
7	7713454	7713447	385430	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000008_0
8	7713443	7713435	386846	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000002_0
10	7713444	7713434	385365	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000000_0
11	7713451	7713440	386060	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000013_0
14	7713442	7713428	385988	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000003_0
16	7713428	7713412	385674	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000001_0
21	7713433	7713412	385586	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000015_0
22	7713440	7713418	386188	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000006_0
23	7713436	7713413	384636	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000004_0
31	7713453	7713422	385391	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000009_0
33	7713384	7713351	384805	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000012_0
34	7713448	7713414	385349	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000014_0
47	7713423	7713376	385814	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000010_0
53	7713413	7713360	385143	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000007_0
76	7713446	7713370	385881	hdfs://nn1:8020/user/hive/warehouse/dw_2g.db/tmp_dim_merchant/corp_id=10001/000018_0
 */


drop  table if exists dw_2g.dim_merchant ;

alter  table  dw_2g.tmp_dim_merchant rename to dw_2g.dim_merchant;


CREATE TABLE if not exists dw_2g.tmp_dim_merchant (
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



insert into table dw_2g.tmp_dim_merchant
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
       dw_2g.dim_merchant
    union all
    select
        cast( ttt1.add_merchant_id + ttt2.max_id as int ) as merchant_id
        , ttt1.mcht_cd
        , ttt1.reg_prov_cd, ttt1.reg_city_cd, ttt1.reg_country_cd
        , ttt1.mcht_type, ttt1.bloc_mcht_cd
        , ttt1.legal_person_id_no, ttt1.mcht_business_license
        , ttt1.corp_id
    from
    (
        select
            row_number() OVER() as add_merchant_id
            , dwd_merchant_info.mcht_cd
            , dwd_merchant_info.reg_prov_cd, dwd_merchant_info.reg_city_cd, dwd_merchant_info.reg_country_cd
            , dwd_merchant_info.mcht_type, dwd_merchant_info.bloc_mcht_cd
            , dwd_merchant_info.legal_person_id_no, dwd_merchant_info.mcht_business_license
            , dwd_merchant_info.corp_id
        from
            dw_2g.dwd_merchant_info
        left join
            dw_2g.dim_merchant
        on dwd_merchant_info.mcht_cd=dim_merchant.mcht_cd
        where dim_merchant.mcht_cd is null
    ) ttt1
    left join
    (
        select
            coalesce(max(cast(merchant_id as int)),0) as max_id
        from dw_2g.dim_merchant
    ) ttt2
) aaa
DISTRIBUTE BY mcht_cd
SORT BY merchant_id
;

/*
初始化商户表
/ user/ hive/ warehouse/ dw_2g.db/ dim_merchant/ corp_id=10001/ 000000_0
"10","55182805039YY0P","62","06","00","5039","","622301198706086755","92620602MA74K75H2F"
"12","55170905641YYV3","52","24","00","5641","","522422198711164424",""
"15","55165505921YXNY","51","03","00","5921","","430682198706144051",""
"45","55146605969YY5A","37","16","00","5969","","372324196901183749",""
"50","55165107011YYFN","51","01","00","7011","","510902198201215331","510124000097693"
"57","55171505995YXJE","52","27","00","5995","","522701199302202214","92522701MA6HHLLM7T"
"106","55145105722YZ44","37","01","00","5722","","130527197402211424","91370105MA3F5LP7P"
"116","55188107299YY1J","65","01","00","7299","","652301197602090312",""
"166","55174307298YXPH","53","25","00","7298","","532529199204052133","92532526MA6NEMQ51C"
"188","55146105311YY00","37","08","00","5311","","37082819930901262X",""
"190","55143107629YXNN","36","09","00","7629","","362227196608080021","92360922L38127501N
==
"7713208","821150151370565","15","01","02","5137","","15263019760825017X","150102600117305"
"7713219","821150151370547","15","01","02","5137","","150104198805201652","150103000005224"
"7713234","821150151370529","15","01","04","5137","","150426197010114221","150104000014035"
"7713251","821150151370510","15","01","02","5137","","150103197803021534","150102600316895"
"7713286","821150151370466","15","01","04","5137","","150122197912060611","150104600256563"
"7713332","821150151370385","15","01","01","5137","","152301197111231020","152301197111231020"
"7713377","821150151370312","15","01","05","5137","","150221198201040319","150105600234972"
"7713393","821150151370286","15","01","03","5137","","331081198410103742","331081198410103742"
"7713429","821150151370231","15","01","23","5137","","15010519691029152x","150194600007510"
"7713438","401264007636911","23","02","00","0763","","230712198708090527",""
"7713444","821150151370213","15","01","03","5137","","362531192402153016","150103600182024"


初始化商户表
/ user/ hive/ warehouse/ dw_2g.db/ dim_merchant/ corp_id=10001/ 000017_0
"6","821371558120248","37","15","23","5812","","372524197603250232","92371523MA3NXR9713"
"35","55167307999YXNC","51","13","00","7999","","512930194508171167",""
"39","55145205532YYE3","37","02","00","5532","","370283198110142619","92370213MA3NK1HN1R"
"49","55165704458YXKA","51","05","00","4458","","510504198810100329",""
"66","55188107011YYSC","65","01","00","7011","","410421196801014558","650105604032628"
"67","55173105699YYGG","53","01","00","5699","","530122197406272647","530122650004045"
"103","55146306513YY5M","37","09","00","6513","","370102197012042923","91370900681730227A"
"124","55161105399YXL1","45","01","00","5399","","450204197706041413","91450105310198593M"
"126","55143105331YY2D","36","09","00","5331","","362201197602104820","92360902MA38ADK31G"
"143","55166505651YXST","51","11","00","5651","","511111196912205539",""
"144","55165705311YXR5","51","05","00","5311","","510521198101275384",""
"206","55142105812YYZ1","36","01","00","5812","","35220219730308051X","92360108MA385D3NXN"
"221","55165700742YYN1","51","05","00","0742","","510503200207053058",""
"258","55119100763YY05","15","01","00","0763","","142131195708014017",""
"269","55158107299YZ3U","44","01","00","7299","","441224197312070021","91440101MA5CJEYQ80"
"293","55158408299YYXW","44","03","00","8299","","440582197904290051","914403000812646800"
==
"7713176","821150151370607","15","01","03","5137","","152529198904222118","150103600313625"
"7713182","821150151370599","15","01","01","5137","        ","150105198512202148","150105198512202148"
"7713237","821150151370526","15","01","02","5137","","152627198901094926","150102600310162"
"7713253","821150151370508","15","01","02","5137","","150103198004092015","150102600317092"
"7713277","55053704789Q8C6","42","10","00","4789","","421087194806152120",""
"7713301","821150151370445","15","01","02","5137","","15012519741211001X","150102600195744"
"7713310","821150151370427","15","01","02","5137","","150121197705075013","150102600319764"
"7713335","821150151370382","15","01","25","5137","","150125197808164225","150125600015098"
"7713396","821150151370283","15","01","04","5137","","150102196506070515","150104600178006"
"7713403","100881059775471","65","01","00","5977","","",""
"7713419","821150151370247","15","01","04","5137","","150102196610094533","150104600095925"
"7713447","821150151370210","15","01","05","5137","","152130198304082408","150105600207327"



添加增量商户数据后
/ user/ hive/ warehouse/ dw_2g.db/ tmp_dim_merchant/ corp_id=10001/ 000000_0
==
==
"7713393","821150151370286","15","01","03","5137","","331081198410103742","331081198410103742"
"7713429","821150151370231","15","01","23","5137","","15010519691029152x","150194600007510"
"7713438","401264007636911","23","02","00","0763","","230712198708090527",""
"7713444","821150151370213","15","01","03","5137","","362531192402153016","150103600182024"
==
"7713455","821620193110105","62","01","02","9311","","620103196901260318","916201027103472362"
"7713456","821620193110097","62","01","02","9311","","620103196901260318","916201027103472362"
"7713457","821611060510188","61","10","23","6051","","612522198310213018","612522198310213018"
"7713458","821611015204001","45","01","03","1520","","440520197603134515","9145010356677833XX"
"7713459","821610960510120","61","09","02","6051","","612401197603300746","612401197603300746"
"7713460","821610860510271","61","08","02","6051","","612701199102013410","612701199102013410"
"7713461","821610860510253","61","08","26","6051","","612727199605050065","612727199605050065"
"7713462","821610860510235","61","08","25","6051","","612726198007160327","612726198007160327"
"7713463","821610760510159","61","07","02","6051","","612301199711063964","612301199711063964"
"7713464","821610760510140","61","07","26","6051","","612326197105190929","612326197105190929"
"7713465","821610660510145","61","06","25","6051","","610625198603020220","610625198603020220"
==
==
"7736314","55073601520FU2G","53","03","00","1520","","532224198207040020",""
"7736315","55073401520FTQE","53","06","00","1520","","53212519860115115X",""
"7736316","55073104816ETDE","53","01","00","4816","","51011119650901001X","9153010007764703XE"
"7736317","55063505998FVMN","45","13","00","5998","","51102819580530511X",""
"7736318","55062805331FURS","45","12","00","5331","","451202199809101316",""
"7736319","55062805311FU83","45","12","00","5311","","452702199105172069",""
"7736320","55062605699FUMC","45","10","00","5699","","452624198502162356","92451023MA5L7PAD12"
"7736321","55061100763MTWV","45","01","00","0763","","452123200203303717",""
"7736322","55052101520MTW2","42","01","00","1520","","420102197505062847",""
"7736323","550473048168KY1","37","13","00","4816","","372801197409024014","913713020659213536"



添加增量商户数据后
/ user/ hive/ warehouse/ dw_2g.db/ tmp_dim_merchant/ corp_id=10001/ 000017_0
==
"7713176","821150151370607","15","01","03","5137","","152529198904222118","150103600313625"
"7713182","821150151370599","15","01","01","5137","        ","150105198512202148","150105198512202148"
"7713237","821150151370526","15","01","02","5137","","152627198901094926","150102600310162"
"7713253","821150151370508","15","01","02","5137","","150103198004092015","150102600317092"
"7713277","55053704789Q8C6","42","10","00","4789","","421087194806152120",""
"7713301","821150151370445","15","01","02","5137","","15012519741211001X","150102600195744"
"7713310","821150151370427","15","01","02","5137","","150121197705075013","150102600319764"
"7713335","821150151370382","15","01","25","5137","","150125197808164225","150125600015098"
"7713396","821150151370283","15","01","04","5137","","150102196506070515","150104600178006"
"7713403","100881059775471","65","01","00","5977","","",""
"7713419","821150151370247","15","01","04","5137","","150102196610094533","150104600095925"
"7713447","821150151370210","15","01","05","5137","","152130198304082408","150105600207327"
==
"7968163","821620193110102","62","01","04","9311","","620103198709063014","91620104MA73L25K8B"
"7968164","821620193110094","62","01","23","9311","","620123198311241728","91620100556294251W"
"7968165","821620159700004","62","01","01","5970","","510623197406058325","916201003253597169"
"7968166","821620153100000","62","01","02","5310","","622301196203160010","91620000924330683R"
"7968167","821611060510185","61","10","25","6051","","612526198912110214","612526198912110214"
"7968168","821611060510167","61","10","23","6051","","612524196701195631","612524196701195631"
"7968169","821610960510118","61","09","02","6051","","612401199305284046","612401199305284046"
"7968170","821610860510269","61","08","24","6051","","612725198411010439","612725198411010439"
"7968171","821610860510250","61","08","23","6051","","612724198311161524","612724198311161524"
===
===
"7989083","552134057121BSH","13","06","00","5712","","130633198809120011","92130633MA093DL275"
"7989084","552134055412GKV","13","06","00","5541","","130638197009011527","91130638MA0D2JLY3P"
"7989085","5521340521130PG","13","06","00","5211","","132401197809273777","91130682MA07XB4171"
"7989086","552134048164E8S","13","06","00","4816","","420881198512180743","91130606MA09F8AX2H"
"7989087","552134040111KC2","13","06","00","4011","","13060219750525031X",""
"7989088","552134040111K83","13","06","00","4011","","220322197509053816",""
"7989089","552134040111D17","13","06","00","4011","","130602199011041816",""
"7989090","552134040111BRJ","13","06","00","4011","","130682198405190635","91130682MA099ULG0K"
"7989091","552134040110HJC","13","06","00","4011","","130637199108011533",""
"7989092","552134040110CH6","13","06","00","4011","","142427197602053633",""
"7989093","552134040110CDN","13","06","00","4011","","130621198604216620",""
"7989094","552134040110BGB","13","06","00","4011","","130627199610111852",""
"7989095","55213404011082W","13","06","00","4011","","130631198309240217",""


/ user/ hive/ warehouse/ dw_2g.db/ tmp_dim_merchant/ corp_id=10001/ 000019_0
==
==
"8083553","55074900780MTHA","53","28","00","0780","","430426197605077367",""
"8083554","55073605311MU03","53","03","00","5311","","532233196503151726",""
"8083555","55073400763MU10","53","06","00","0763","","532128198806153123",""
"8083556","55073104816NJ47","53","01","00","4816","","530111198209232035","91530103343641686H"
"8083557","55071506012FTNG","52","27","00","6012","","522725196506201632","915227012162814549"
"8083558","55070105814FULF","52","01","00","5814","","520122199509184145","92520122MA6E81GP3Q"
"8083559","55063307298FUNB","45","08","00","7298","","45252319751205087X",""
"8083560","55062800763MTPX","45","12","00","0763","","452701198807092441",""
"8083561","550541048168K2W","42","28","00","4816","","42011119750525559X","91422802183110578T"
"8083562","55052201520MU2P","42","02","00","1520","","42028119840328572X",""
"8083563","550452048168J7X","37","02","00","4816","","370802198404153932","91370202MA3CDQLQ10"
"8083564","309210258138001","21","02","04","5813","","210203196104072516","2102002115026"

*/