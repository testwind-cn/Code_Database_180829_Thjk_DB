select * from ( SELECT row_number() over (partition  BY mchtcd,branchcd order by file_date desc ) as r_num  ,* FROM branch_apms_bl ) as a where  a.r_num=1 ORDER BY a.mchtcd,a.branchcd,a.r_num;


-- 324793


select count(*) from branch_apms_bl;
-- 514320


-- Branch_APMS_2nd_20160701.txt
--  共执行了922天：因为缺了 20171010 的文件； 2016-10-19 有多的两个补数文件
--  1、20190110 191    2019-1-9    2018-7-3
--  3、20180703 265    2018-7-2    2017-10-11
--  2、20171010 466    2017-10-9   2016-7-1

select * from ( SELECT row_number() over (partition  BY mchtcd,branchcd order by file_date desc ) as r_num  ,* FROM branch_apms_bl ) as a where  a.r_num=1 ORDER BY a.mchtcd,a.branchcd,a.r_num;
-- # 数量 1084842


select * from (SELECT row_number()over(partition BY bc.mchtcd, bc.branchcd order by bc.file_date desc) as r_num, * FROM (SELECT * from rds_posflow.branch_apms_bl where to_date(concat(substr(file_date, 1, 4), '-', substr(file_date, 5, 2), '-', substr(file_date, 7, 2), ' 00:00:00')) < to_date('2018-01-01 00:00:00')) as bc) as ba where ba.r_num = 1 ORDER BY ba.mchtcd, ba.branchcd, ba.r_num;

-- 去除2018年以前的每个分店的唯一的最新操作记录

