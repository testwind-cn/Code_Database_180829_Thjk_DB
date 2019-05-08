SELECT CASE
           WHEN `col_47` > 30
               AND `col_47` <= 60 THEN '（30-60]天'
           WHEN `col_47` > 60
               AND `col_47` <= 90 THEN '（60-90]天'
           WHEN `col_47` > 90
               AND `col_47` <= 180 THEN '（90-180]天'
           WHEN `col_47` > 180
               AND `col_47` <= 270 THEN '（180-270]天'
           WHEN `col_47` > 270
               AND `col_47` <= 360 THEN '（270-360]天'
           WHEN `col_47` > 360 THEN
               '(360,)天'
           ELSE '[0,30]天'
           END       AS `类型`,
       -- 	count( * ),
       sum(1)        AS `户数`,
       sum(`col_40`)/10000.0 AS `应收本金余额（万元）`
FROM d0009
WHERE  `col_47` > 30  -- 只要逾期天数大于30天的
  AND `col_09` < '2018-12-31'  -- 在报表日期时已经到期了
  AND data_dt = '2018-12-31'   -- 报表日期
GROUP BY CASE
             WHEN `col_47` > 30
                 AND `col_47` <= 60 THEN '（30-60]天'
             WHEN `col_47` > 60
                 AND `col_47` <= 90 THEN '（60-90]天'
             WHEN `col_47` > 90
                 AND `col_47` <= 180 THEN '（90-180]天'
             WHEN `col_47` > 180
                 AND `col_47` <= 270 THEN '（180-270]天'
             WHEN `col_47` > 270
                 AND `col_47` <= 360 THEN '（270-360]天'
             WHEN `col_47` > 360 THEN
                 '(360,)天'
             ELSE '[0,30]天'
             END
ORDER BY `col_47`;


/*
SELECT ((`col_47` - 1) div 30) as `类型`,
       count(*),
       sum(1)                  as `户数`,
       sum(`col_40`)           as `应收本金余额`
FROM d0009
WHERE `col_47` > 30
  and `col_09` < '2018-12-31'
  and data_dt = '2018-12-31'

-- 	DAYOFMONTH( DATE_ADD( data_dt, INTERVAL ( 1 ) DAY ) ) = 1 
GROUP BY ((`col_47` - 1) div 30);
*/


-- 1 = 30-60
-- 2 = 60-90

--  3 = 90-120
-- 4 = 120-150
-- 5 = 150-180

-- 6 = 180-210
-- 7 = 210-240
-- 8 = 240-270

-- 9 = 270-300
-- 10 = 300-330
-- 11 = 330-360

-- 12 = 

	
