SELECT
    gm1.mbmd5 as mobile_md5,
    md5(gm1.mcht_cd) as mcht_cd_md5,
    gm1.login_mobile,
    gm1.mcht_cd,
    t3.*
from
(
    select
        t2.mcht_cd,

        sum(if(t2.inst_date='2017-09-01',t2.flow_amt_sell,0)) as flow_amt_sell_20170901,
        sum(if(t2.inst_date='2017-09-01',t2.flow_vol_sell,0)) as flow_vol_sell_20170901,

        sum(if(t2.inst_date='2017-10-01',t2.flow_amt_sell,0)) as flow_amt_sell_20171001,
        sum(if(t2.inst_date='2017-10-01',t2.flow_vol_sell,0)) as flow_vol_sell_20171001,

        sum(if(t2.inst_date='2017-11-01',t2.flow_amt_sell,0)) as flow_amt_sell_20171101,
        sum(if(t2.inst_date='2017-11-01',t2.flow_vol_sell,0)) as flow_vol_sell_20171101,

        sum(if(t2.inst_date='2017-12-01',t2.flow_amt_sell,0)) as flow_amt_sell_20171201,
        sum(if(t2.inst_date='2017-12-01',t2.flow_vol_sell,0)) as flow_vol_sell_20171201,

        sum(if(t2.inst_date='2018-01-01',t2.flow_amt_sell,0)) as flow_amt_sell_20180101,
        sum(if(t2.inst_date='2018-01-01',t2.flow_vol_sell,0)) as flow_vol_sell_20180101,

        sum(if(t2.inst_date='2018-02-01',t2.flow_amt_sell,0)) as flow_amt_sell_20180201,
        sum(if(t2.inst_date='2018-02-01',t2.flow_vol_sell,0)) as flow_vol_sell_20180201,

        sum(if(t2.inst_date='2018-03-01',t2.flow_amt_sell,0)) as flow_amt_sell_20180301,
        sum(if(t2.inst_date='2018-03-01',t2.flow_vol_sell,0)) as flow_vol_sell_20180301,

        sum(if(t2.inst_date='2018-04-01',t2.flow_amt_sell,0)) as flow_amt_sell_20180401,
        sum(if(t2.inst_date='2018-04-01',t2.flow_vol_sell,0)) as flow_vol_sell_20180401,

        sum(if(t2.inst_date='2018-05-01',t2.flow_amt_sell,0)) as flow_amt_sell_20180501,
        sum(if(t2.inst_date='2018-05-01',t2.flow_vol_sell,0)) as flow_vol_sell_20180501,

        sum(if(t2.inst_date='2018-06-01',t2.flow_amt_sell,0)) as flow_amt_sell_20180601,
        sum(if(t2.inst_date='2018-06-01',t2.flow_vol_sell,0)) as flow_vol_sell_20180601,

        sum(if(t2.inst_date='2018-07-01',t2.flow_amt_sell,0)) as flow_amt_sell_20180701,
        sum(if(t2.inst_date='2018-07-01',t2.flow_vol_sell,0)) as flow_vol_sell_20180701,

        sum(if(t2.inst_date='2018-08-01',t2.flow_amt_sell,0)) as flow_amt_sell_20180801,
        sum(if(t2.inst_date='2018-08-01',t2.flow_vol_sell,0)) as flow_vol_sell_20180801,

        sum(if(t2.inst_date='2018-09-01',t2.flow_amt_sell,0)) as flow_amt_sell_20180901,
        sum(if(t2.inst_date='2018-09-01',t2.flow_vol_sell,0)) as flow_vol_sell_20180901,

        sum(if(t2.inst_date='2018-10-01',t2.flow_amt_sell,0)) as flow_amt_sell_20181001,
        sum(if(t2.inst_date='2018-10-01',t2.flow_vol_sell,0)) as flow_vol_sell_20181001,

        sum(if(t2.inst_date='2018-11-01',t2.flow_amt_sell,0)) as flow_amt_sell_20181101,
        sum(if(t2.inst_date='2018-11-01',t2.flow_vol_sell,0)) as flow_vol_sell_20181101,

        sum(if(t2.inst_date='2018-12-01',t2.flow_amt_sell,0)) as flow_amt_sell_20181201,
        sum(if(t2.inst_date='2018-12-01',t2.flow_vol_sell,0)) as flow_vol_sell_20181201,

        sum(if(t2.inst_date='2019-01-01',t2.flow_amt_sell,0)) as flow_amt_sell_20190101,
        sum(if(t2.inst_date='2019-01-01',t2.flow_vol_sell,0)) as flow_vol_sell_20190101,

        sum(if(t2.inst_date='2019-02-01',t2.flow_amt_sell,0)) as flow_amt_sell_20190201,
        sum(if(t2.inst_date='2019-02-01',t2.flow_vol_sell,0)) as flow_vol_sell_20190201,

        sum(if(t2.inst_date='2019-03-01',t2.flow_amt_sell,0)) as flow_amt_sell_20190301,
        sum(if(t2.inst_date='2019-03-01',t2.flow_vol_sell,0)) as flow_vol_sell_20190301,

        sum(if(t2.inst_date='2019-04-01',t2.flow_amt_sell,0)) as flow_amt_sell_20190401,
        sum(if(t2.inst_date='2019-04-01',t2.flow_vol_sell,0)) as flow_vol_sell_20190401,

        sum(if(t2.inst_date='2019-05-01',t2.flow_amt_sell,0)) as flow_amt_sell_20190501,
        sum(if(t2.inst_date='2019-05-01',t2.flow_vol_sell,0)) as flow_vol_sell_20190501,

        sum(if(t2.inst_date='2019-06-01',t2.flow_amt_sell,0)) as flow_amt_sell_20190601,
        sum(if(t2.inst_date='2019-06-01',t2.flow_vol_sell,0)) as flow_vol_sell_20190601,

        sum(if(t2.inst_date='2019-07-01',t2.flow_amt_sell,0)) as flow_amt_sell_20190701,
        sum(if(t2.inst_date='2019-07-01',t2.flow_vol_sell,0)) as flow_vol_sell_20190701,

        sum(if(t2.inst_date='2019-08-01',t2.flow_amt_sell,0)) as flow_amt_sell_20190801,
        sum(if(t2.inst_date='2019-08-01',t2.flow_vol_sell,0)) as flow_vol_sell_20190801,

        sum(if(t2.inst_date='2019-09-01',t2.flow_amt_sell,0)) as flow_amt_sell_20190901,
        sum(if(t2.inst_date='2019-09-01',t2.flow_vol_sell,0)) as flow_vol_sell_20190901,

        sum(if(t2.inst_date='2019-10-01',t2.flow_amt_sell,0)) as flow_amt_sell_20191001,
        sum(if(t2.inst_date='2019-10-01',t2.flow_vol_sell,0)) as flow_vol_sell_20191001,

        sum(if(t2.inst_date='2019-11-01',t2.flow_amt_sell,0)) as flow_amt_sell_20191101,
        sum(if(t2.inst_date='2019-11-01',t2.flow_vol_sell,0)) as flow_vol_sell_20191101,

        sum(if(t2.inst_date='2019-12-01',t2.flow_amt_sell,0)) as flow_amt_sell_20191201,
        sum(if(t2.inst_date='2019-12-01',t2.flow_vol_sell,0)) as flow_vol_sell_20191201,

        sum(if(t2.inst_date='2020-01-01',t2.flow_amt_sell,0)) as flow_amt_sell_20200101,
        sum(if(t2.inst_date='2020-01-01',t2.flow_vol_sell,0)) as flow_vol_sell_20200101,

        sum(if(t2.inst_date='2020-02-01',t2.flow_amt_sell,0)) as flow_amt_sell_20200201,
        sum(if(t2.inst_date='2020-02-01',t2.flow_vol_sell,0)) as flow_vol_sell_20200201,

        sum(if(t2.inst_date='2020-03-01',t2.flow_amt_sell,0)) as flow_amt_sell_20200301,
        sum(if(t2.inst_date='2020-03-01',t2.flow_vol_sell,0)) as flow_vol_sell_20200301,

        sum(if(t2.inst_date='2020-04-01',t2.flow_amt_sell,0)) as flow_amt_sell_20200401,
        sum(if(t2.inst_date='2020-04-01',t2.flow_vol_sell,0)) as flow_vol_sell_20200401,

        sum(if(t2.inst_date='2020-05-01',t2.flow_amt_sell,0)) as flow_amt_sell_20200501,
        sum(if(t2.inst_date='2020-05-01',t2.flow_vol_sell,0)) as flow_vol_sell_20200501,

        sum(if(t2.inst_date='2020-06-01',t2.flow_amt_sell,0)) as flow_amt_sell_20200601,
        sum(if(t2.inst_date='2020-06-01',t2.flow_vol_sell,0)) as flow_vol_sell_20200601
    from rds_posflow.bl_flow_by_month as t2
    where t2.inst_date >= '2017-07-01' and t2.inst_date < '2020-07-01'
    group by mcht_cd
) t3
LEFT JOIN
(
    select gm.tid , gm.mcht_cd , gm.mcht_type, gum1.user_id, gum1.mcht_tid ,gum1.login_mobile,gum1.mbmd5
    from ods_cd_goodboss.gb_mcht_info as gm
    LEFT JOIN
    (
        SELECT gum.user_id, gum.mcht_tid ,gu.login_mobile,gu.mbmd5
        from ods_cd_goodboss.gb_user_mcht as gum
        LEFT JOIN
        (
            SELECT gu.user_id,gu.login_mobile,md5(gu.login_mobile) as mbmd5
            FROM ods_cd_goodboss.gb_user as gu
            LEFT SEMI JOIN  tmp_db.fengqile on md5(gu.login_mobile) = fengqile.md5
        ) as gu
        on gum.user_id = gu.user_id
        where gu.user_id is NOT NULL
    ) as gum1
    on gm.tid = gum1.mcht_tid
    where gum1.mcht_tid is NOT NULL
) gm1
on t3.mcht_cd = gm1.mcht_cd
where
gm1.mcht_cd is NOT NULL
ORDER BY
    mobile_md5,mcht_cd_md5