select mob.login_mobile, mob.user_id, card.*
from (

         --  用户手机号码 3359
         select *
         from gb_user
         where user_id in (
                           191838, 20937, 46655, 187533, 18101, 118611)
            or user_id in
               (select aaa.user_id
                from (SELECT sss.*,
                             mmm.mcht_cd       mmcd,
                             mmm.tid,
                             uumm.user_id,
                             mmm.is_delete  AS d1,
                             uumm.is_delete AS d2,
                             uumm.STATUS    AS s1
                      FROM (SELECT * FROM s6700 WHERE data_dt = '2019-04-29' and overdue_days < 60) sss
                               LEFT JOIN `gb_mcht_info` mmm ON sss.mcht_cd = mmm.mcht_cd
                               LEFT JOIN gb_user_mcht uumm ON mmm.tid = uumm.mcht_tid

                      HAVING mmm.is_delete = 0
                         AND uumm.is_delete = 0
                         AND (uumm.STATUS = 1)
                     ) aaa)
         --  用户手机号码
     ) mob

         left join (


-- 805
    SELECT *
    FROM `gb_withholding_card`
    where status = 1
      and is_delete = 0
      and payment_default = 0
      and `signed_status` = 1
      and user_id in (
        select user_id
        from (
                 SELECT *
                 FROM `gb_withholding_card`
                 where status = 1
                   and is_delete = 0
--                              and payment_default = 1
                   and `signed_status` = 1
             ) aaa
        group by user_id
        having count(user_id) = 1
    )
        union
        SELECT * FROM `gb_withholding_card`
        where status = 1
      and is_delete = 0
      and payment_default = 1
      and `signed_status` = 1
) card on card.user_id = mob.user_id

where card.bank_id is null
   or bank_name like '%农%';


-- -------


SELECT sss.*,
       mmm.mcht_cd       mmcd,
       mmm.tid,
       uumm.user_id,
       mmm.is_delete  AS d1,
       uumm.is_delete AS d2,
       uumm.STATUS    AS s1
FROM (SELECT *
      FROM s6700
      WHERE data_dt = '2019-04-28'
        and mcht_cd in ('821371759981668',
                        '821511859980079',
                        '8217970829900UR',
                        '821610151390113',
                        '821371657220085',
                        '821152256410016',
                        '821150551390013',
                        '821420548120018',
                        '821510850390078',
                        '821522750650033')
     ) sss
         LEFT JOIN `gb_mcht_info` mmm ON sss.mcht_cd = mmm.mcht_cd
         LEFT JOIN gb_user_mcht uumm ON mmm.tid = uumm.mcht_tid
HAVING mmm.is_delete = 0
   AND uumm.is_delete = 0
-- AND (uumm.STATUS = 1)

