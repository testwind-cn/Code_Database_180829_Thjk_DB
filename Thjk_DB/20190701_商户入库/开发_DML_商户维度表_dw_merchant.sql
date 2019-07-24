-- CREATE TABLE `dw_2g`.`dim_merchant` (
CREATE TABLE `dw_2g`.`dim_merchant_tmp` (
  `merchant_id` int COMMENT '记录编号', -- HIVE建表
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



select mcht_cd from dw_2g.dwd_merchant_info limit 10;


insert into table `dw_2g`.`dim_merchant`
select row_number() OVER() as merchant_id, mcht_cd, '100001' as `corp_id`
from dw_2g.dwd_merchant_info limit 1000000;

select * from  `dw_2g`.`dim_merchant` where merchant_id > 999959 or merchant_id < 50 order by cast(merchant_id as int);

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


insert into table dw_2g.dim_merchant_tmp
select
   merchant_id,mcht_cd,corp_id
from
   dw_2g.dim_merchant
union all
select
    cast( ( row_number() OVER() ) + ttt2.max_id as int ) as merchant_id,ttt1.mcht_cd,'100001' as corp_id
from
(
    select
        dwd_merchant_info.mcht_cd
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
        coalesce(max(cast(merchant_id as int)),0)    max_id
    from dw_2g.dim_merchant
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



