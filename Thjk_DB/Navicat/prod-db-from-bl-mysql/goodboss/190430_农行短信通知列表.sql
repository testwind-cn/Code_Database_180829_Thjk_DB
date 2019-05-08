-- 4576
-- 全部 默认卡=1 的，是  774（1人多卡取默认） + 3802（1人1卡）
SELECT *
FROM `gb_withholding_card`
where status = 1
  and is_delete = 0
  and payment_default = 1
  and `signed_status` = 1;


-- 6453个卡
-- 4607（1人1卡）+1846（1人多卡，其实878个人）
SELECT *
FROM `gb_withholding_card`
where status = 1
  and is_delete = 0
--                              and payment_default = 1
  and `signed_status` = 1;


-- 5485个人，（其实有 6453个卡，不管是否默认卡）
select user_id
from (
         SELECT *
         FROM `gb_withholding_card`
         where status = 1
           and is_delete = 0
--                              and payment_default = 1
           and `signed_status` = 1
     ) aaa
group by user_id;


-- 878人，（这些有人多张卡，共1846张卡，有默认和非默认）
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
having count(user_id) > 1;


-- 4607人卡，（这些有人只有1张卡，有默认和非默认）
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
having count(user_id) = 1;


-- 4607人卡，（这些有人只有1张卡，有默认和非默认）
SELECT *
FROM `gb_withholding_card`
where status = 1
  and is_delete = 0
--                              and payment_default = 1
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
) order by payment_default;


-- 3802人卡，（这些有人只有1张卡，是默认 = 1）
SELECT *
FROM `gb_withholding_card`
where status = 1
  and is_delete = 0
  and payment_default = 1
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
) order by payment_default;


-- 805人卡，（这些有人只有1张卡，是非默认 = 0）
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
) order by payment_default;


-- 1846卡，这些是有多张卡的，有默认=1和非默认=0
SELECT *
FROM `gb_withholding_card`
where status = 1
  and is_delete = 0
--                              and payment_default = 1
  and `signed_status` = 1
  and user_id in
      ( -- 878
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
          having count(user_id) > 1
      );


-- 774卡，这些是有多张卡的，默认=1和非默认=0
SELECT *
FROM `gb_withholding_card`
where status = 1
  and is_delete = 0
  and payment_default = 1
  and `signed_status` = 1
  and user_id in
      ( -- 878
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
          having count(user_id) > 1
      );


-- 1072卡，这些是有多张卡的，非默认=0
SELECT *
FROM `gb_withholding_card`
where status = 1
  and is_delete = 0
  and payment_default = 0
  and `signed_status` = 1
  and user_id in
      ( -- 878
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
          having count(user_id) > 1
      );