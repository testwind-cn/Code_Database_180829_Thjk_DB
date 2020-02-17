select
1 as aa
,add_months(create_time,-5) as flow_dt
, create_time
, CAST( regexp_extract(flow_amt_coord,'"6M":\\["[\\d\\.]*","[\\d\\.]*","[\\d\\.]*","[\\d\\.]*","[\\d\\.]*","([\\d\\.]*)"',1) AS DECIMAL(30,2)) as flow_amt_6M_u
, CAST( regexp_extract(flow_amt_coord,'"6M":\\["[\\d\\.]*","[\\d\\.]*","[\\d\\.]*","[\\d\\.]*","[\\d\\.]*","([\\d\\.]*)"',1) AS DECIMAL(30,2)) as flow_amt_6M_6
, CAST( regexp_extract(flow_amt_coord,'"6M":\\["([\\d\\.]*)"',1) AS DECIMAL(30,2)) as flow_amt_6M_1
-- ,CAST( regexp_extract(flow_vol,'"3M":"([\\d\\.]*)',1) AS DECIMAL(30,2)) as flow_vol_3M
-- ,CAST( regexp_extract(flow_vol,'"6M":"([\\d\\.]*)',1) AS DECIMAL(30,2)) as flow_vol_6M
, flow_amt_coord,flow_vol
from entry_pos_flow
where
create_time in ( '2019-02-28','2019-03-28','2019-04-28','2019-05-28','2019-06-28','2019-07-28')
and  merchant_ap='149505059991867'
order by create_time

union all

select
2 as aa
, create_time as flow_dt
, create_time
, CAST( regexp_extract(flow_amt_coord,'"6M":\\["([\\d\\.]*)"',1) AS DECIMAL(30,2)) as flow_amt_6M_u
, CAST( regexp_extract(flow_amt_coord,'"6M":\\["[\\d\\.]*","[\\d\\.]*","[\\d\\.]*","[\\d\\.]*","[\\d\\.]*","([\\d\\.]*)"',1) AS DECIMAL(30,2)) as flow_amt_6M_6
, CAST( regexp_extract(flow_amt_coord,'"6M":\\["([\\d\\.]*)"',1) AS DECIMAL(30,2)) as flow_amt_6M_1
-- ,CAST( regexp_extract(flow_vol,'"3M":"([\\d\\.]*)',1) AS DECIMAL(30,2)) as flow_vol_3M
-- ,CAST( regexp_extract(flow_vol,'"6M":"([\\d\\.]*)',1) AS DECIMAL(30,2)) as flow_vol_6M
, flow_amt_coord,flow_vol
from entry_pos_flow
where
create_time in ( -- '2019-02-28',
     '2019-03-28','2019-04-28','2019-05-28','2019-06-28','2019-07-28')
and  merchant_ap='149505059991867'
order by create_time
;




















select
    flow_dt
    ,flow_amt_6M_u
    ,flow_amt_6M_n
--    ,merchant_ap
from (

    select
        add_months(create_time,-5) as flow_dt
        , sum( CAST( regexp_extract(flow_amt_coord,'"6M":\\["[\\d\\.]*","[\\d\\.]*","[\\d\\.]*","[\\d\\.]*","[\\d\\.]*","([\\d\\.]*)"',1) AS DECIMAL(30,2)) ) as flow_amt_6M_u
        , sum( if( CAST( regexp_extract(flow_amt_coord,'"6M":\\["[\\d\\.]*","[\\d\\.]*","[\\d\\.]*","[\\d\\.]*","[\\d\\.]*","([\\d\\.]*)"',1) AS DECIMAL(30,2))>0,1,0 ) ) as flow_amt_6M_n
--    , merchant_ap
    from entry_pos_flow
    where
        create_time in ( '2019-03-10','2019-04-10','2019-05-10','2019-06-10','2019-07-10','2019-08-10')
    group by create_time

    union all

    select
        create_time as flow_dt
        , sum( CAST( regexp_extract(flow_amt_coord,'"6M":\\["([\\d\\.]*)"',1) AS DECIMAL(30,2)) )as flow_amt_6M_u
        , sum( if(  CAST( regexp_extract(flow_amt_coord,'"6M":\\["([\\d\\.]*)"',1) AS DECIMAL(30,2))>0,1,0 ) )as flow_amt_6M_n
--    , merchant_ap
    from entry_pos_flow
    where
        create_time in ( -- '2019-03-10',
        '2019-04-10','2019-05-10','2019-06-10','2019-07-10','2019-08-10')
    group by create_time
) aaa
order by
    flow_dt


/*

 2-20            as 2-20 -5个月
3-20
4-20
5-20
6-20
7-20		as 7-20 -5个月
-6



7-20 - 180-150   1-20-2-20


data_dt in (20190220,,, 20190720)  data_dt-5


data_dt in (20190320,,, 20190720)  data_dt

3-20		3-20
4-20		4
5-20
6-20
7-20
-1


 */