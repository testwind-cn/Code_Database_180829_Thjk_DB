SELECT date_v,`f_date_ck_e`('2018-05-03',date_v,4,10) from dim_date


SELECT * from 
( SELECT * from dim_date 
where date_v >= '2019-03-01' and date_v<='2019-03-10'
) hh LEFT JOIN (SELECT * FROM test_d) ddd on hh.date_v> ddd.data_dt
ORDER BY hh.date_v;


SELECT bb.date_v, ( sum(`ccc`.`全部结清-笔数` ) + sum( `ccc`.`全部未结清-笔数` ) ) / 14.0
FROM (
       SELECT date_v
       FROM dim_date
       WHERE
--            date_v >= STR_TO_DATE('$DATE_START', '%Y-%m-%d')    -- 2019-03-12
--        AND date_v <= STR_TO_DATE('$DATE_END', '%Y-%m-%d')    -- 2019-05-01
              date_v >= STR_TO_DATE('2015-08-17', '%Y-%m-%d')    -- 2019-03-12
          AND date_v <= STR_TO_DATE('2019-05-01', '%Y-%m-%d')    -- 2019-05-01
       ORDER BY date_v
     ) AS bb
       LEFT JOIN (
         SELECT * from v01 order by tgroup
         )
         AS ccc
       ON   DATEDIFF( bb.date_v, ccc.tgroup ) <14 and DATEDIFF( bb.date_v, ccc.tgroup )>=0
GROUP BY bb.date_v
ORDER BY bb.date_v




-- BEGIN 按周分组
SELECT
	tgroup,
	sum(c1) as  '全部结清-笔数',
	sum(c2) as  '全部结清-原始贷款本金',
	sum(c6) as  '全部未结清-笔数',
	sum(c7) as  '全部未结清-原始贷款本金',
	sum(c8) as  '逾期30天+-笔数',
	sum(c9) as  '逾期30天+原始贷款本金',
	sum(c10) as  '逾期30天+贷款余额.本金',
	sum(c11) as  '逾期90天+-笔数',
	sum(c12) as  '逾期90天+原始贷款本金',
	sum(c13) as  '逾期90天+贷款余额.本金'

from
(
	(
		(
			SELECT 
				col_08 AS tgroup,
			--  YEAR(col_08),
				0 as c1,
				0 as c2,
				sum(1)  as c6, -- '全部未结清-笔数',
				sum(`col_10`) as c7, -- '全部未结清-原始贷款本金', 
				sum(IF(col_47 >= 30,1,0)) as c8, -- '逾期30天+-笔数',
				sum(IF(col_47 >= 30,col_10,0)) as c9, -- '逾期30天+原始贷款本金',
				sum(IF(col_47 >= 30,col_40,0)) as c10, -- '逾期30天+贷款余额.本金',
				sum(IF(col_47 >= 90,1,0)) as c11 , -- '逾期90天+-笔数',
				sum(IF(col_47 >= 90,col_10,0)) as c12 , -- '逾期90天+原始贷款本金',
				sum(IF(col_47 >= 90,col_40,0)) as c13 -- '逾期90天+贷款余额.本金'
				from d0009 WHERE data_dt = '2019-04-04' 
			GROUP BY col_08
			-- GROUP BY YEAR(col_08)
		)
		UNION all
		(
			SELECT 
				col_08 AS tgroup,
			--  YEAR(col_08),
				sum(1) as c1, -- '全部结清-笔数',
				sum(`col_10`) as c2, -- '全部结清-原始贷款本金'
				0 as c6,
				0 as c7,
				0 as c8,
				0 as c9,
				0 as c10,
				0 as c11,
				0 as c12,
				0 as c12
				from d0010 WHERE data_dt = '2019-04-04' 
			GROUP BY col_08
		)
	) as un_data
) GROUP BY tgroup ORDER BY tgroup
-- END 按日分组


-- BEGIN 按周分组
SELECT
	tgroup,
	sum(c1) as  '全部结清-笔数',
	sum(c2) as  '全部结清-原始贷款本金',
	sum(c6) as  '全部未结清-笔数',
	sum(c7) as  '全部未结清-原始贷款本金',
	sum(c8) as  '逾期30天+-笔数',
	sum(c9) as  '逾期30天+原始贷款本金',
	sum(c10) as  '逾期30天+贷款余额.本金',
	sum(c11) as  '逾期90天+-笔数',
	sum(c12) as  '逾期90天+原始贷款本金',
	sum(c13) as  '逾期90天+贷款余额.本金'

from
(
	(		(
			SELECT 
				ADDDATE('2015-01-05',  (DATEDIFF(col_08,'2015-01-05') div 7 )*7  ) AS tgroup,
			--  YEAR(col_08),
				0 as c1,
				0 as c2,
				sum(1)  as c6, -- '全部未结清-笔数',
				sum(`col_10`) as c7, -- '全部未结清-原始贷款本金', 
				sum(IF(col_47 >= 30,1,0)) as c8, -- '逾期30天+-笔数',
				sum(IF(col_47 >= 30,col_10,0)) as c9, -- '逾期30天+原始贷款本金',
				sum(IF(col_47 >= 30,col_40,0)) as c10, -- '逾期30天+贷款余额.本金',
				sum(IF(col_47 >= 90,1,0)) as c11 , -- '逾期90天+-笔数',
				sum(IF(col_47 >= 90,col_10,0)) as c12 , -- '逾期90天+原始贷款本金',
				sum(IF(col_47 >= 90,col_40,0)) as c13 -- '逾期90天+贷款余额.本金'
				from d0009 WHERE data_dt = '2019-04-04' 
			GROUP BY ADDDATE('2015-01-05',  (DATEDIFF(col_08,'2015-01-05') div 7 )*7  )
			-- GROUP BY YEAR(col_08)
		)
		UNION all
		(
			SELECT 
				ADDDATE('2015-01-05',  (DATEDIFF(col_08,'2015-01-05') div 7 )*7  )  AS tgroup,
			--  YEAR(col_08),
				sum(1) as c1, -- '全部结清-笔数',
				sum(`col_10`) as c2, -- '全部结清-原始贷款本金'
				0 as c6,
				0 as c7,
				0 as c8,
				0 as c9,
				0 as c10,
				0 as c11,
				0 as c12,
				0 as c12
				from d0010 WHERE data_dt = '2019-04-04' 
			GROUP BY ADDDATE('2015-01-05',  (DATEDIFF(col_08,'2015-01-05') div 7 )*7  )
		)
	) as un_data
) GROUP BY tgroup ORDER BY tgroup
-- END 按周分组





SELECT count(*) from d0010


-- 入库 343 天数据 2018-04-27  - 2019-04-04
-- SELECT count(*) , data_dt from d0009 GROUP BY data_dt;


-- ---------------------------
-- 有78 个结清的，曾经逾期过90天
-- SELECT * from d0010 where col_07 in ( SELECT DISTINCT(col_07) from d0009 where col_47>=90 )
-- 0 未结清： - 未到期 ,逾期小于7天，曾经逾期过90天
-- SELECT * FROM d0009 where data_dt='2019-04-04' and col_45 = '活动中' and col_47 <7 and col_07 in ( SELECT DISTINCT(col_07) from d0009 where col_47>=90 )
-- 发生过90天逾期的有 1202
-- SELECT DISTINCT(col_07) from d0009 where col_47>=90 


-- 有89 个结清的，曾经逾期过30-90天
-- SELECT * from d0010 where col_07 in ( select bill from ( SELECT DISTINCT(col_07) as bill , max(col_47) as ov_days from d0009 GROUP BY col_07 ) hhh where ov_days>=30 and ov_days < 90 )
-- 16 未结清： - 未到期 ,逾期小于7天，曾经逾期过30-90天
-- SELECT * FROM d0009 where data_dt='2019-04-04' and col_45 = '活动中' and col_47 <7 and col_07 in ( select bill from ( SELECT DISTINCT(col_07) as bill , max(col_47) as ov_days from d0009 GROUP BY col_07 ) hhh where ov_days>=30 and ov_days < 90 ) 
-- 发生过30-90天逾期的有 204
-- select bill from ( SELECT DISTINCT(col_07) as bill , max(col_47) as ov_days from d0009 GROUP BY col_07 ) hhh where ov_days>=30 and ov_days < 90


-- 有282 个结清的，曾经逾期过14-30天
-- SELECT * from d0010 where col_07 in ( select bill from ( SELECT DISTINCT(col_07) as bill , max(col_47) as ov_days from d0009 GROUP BY col_07 ) hhh where ov_days>=14 and ov_days < 30 )
-- 197 未结清： - 未到期 ,逾期小于7天，曾经逾期过14-30天
-- SELECT * FROM d0009 where data_dt='2019-04-04' and col_45 = '活动中' and col_47 <7 and col_07 in ( select bill from ( SELECT DISTINCT(col_07) as bill , max(col_47) as ov_days from d0009 GROUP BY col_07 ) hhh where ov_days>=14 and ov_days < 30 )
-- 发生过14-30天逾期的有 535
-- select bill from ( SELECT DISTINCT(col_07) as bill , max(col_47) as ov_days from d0009 GROUP BY col_07 ) hhh where ov_days>=14 and ov_days < 30


-- 有352 个结清的，曾经逾期过7天
-- SELECT * from d0010 where col_07 in ( select bill from ( SELECT DISTINCT(col_07) as bill , max(col_47) as ov_days from d0009 GROUP BY col_07 ) hhh where ov_days>=7 and ov_days < 14 )
-- 228 未结清： - 未到期 ,逾期小于7天，曾经逾期过7天
-- SELECT * FROM d0009 where data_dt='2019-04-04' and col_45 = '活动中' and col_47 <7 and col_07 in ( select bill from ( SELECT DISTINCT(col_07) as bill , max(col_47) as ov_days from d0009 GROUP BY col_07 ) hhh where ov_days>=7 and ov_days < 14 )
-- 发生过7-14天逾期的有 609
-- select bill from ( SELECT DISTINCT(col_07) as bill , max(col_47) as ov_days from d0009 GROUP BY col_07 ) hhh where ov_days>=7 and ov_days < 14

-- ---------------------------


-- 全部结清的有 13467
-- SELECT count(*) from d0010

-- 4600 未结清： - 到期 、未到期
SELECT * FROM d0009 where data_dt='2019-04-04' 

-- 3432 未结清： - 未到期
SELECT * FROM d0009 where data_dt='2019-04-04' and col_45 = '活动中'  ORDER BY col_47

-- 143 未结清： - 未到期 ,逾期超过7天
-- SELECT * FROM d0009 where data_dt='2019-04-04' and col_45 = '活动中' and col_47 >=7 ORDER BY col_47

-- 3289 未结清： - 未到期 ,逾期小于7天
-- SELECT * FROM d0009 where data_dt='2019-04-04' and col_45 = '活动中' and col_47 <7 ORDER BY col_47

-- 500，未结清，到期
-- SELECT * FROM d0009 where data_dt='2019-04-04' and col_45 = '到期未结清' ORDER BY col_47

