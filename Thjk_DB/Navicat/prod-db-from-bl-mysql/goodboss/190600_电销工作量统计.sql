SELECT count(mcht_cd),merchant_info.* from merchant_info GROUP BY mcht_cd HAVING count(mcht_cd) > 1


SELECT count(*) from merchant_info



select DISTINCT(direct_advisor_id) from gb_loan_advisor where direct_advisor_id is not null; 
-- 15170,15171,53,15238,15239,15371


select * from user where user_id in ( 15170,15171,53,15238,15239,15371 );


select * from gb_loan_advisor where direct_advisor_id is not null;

SELECT * from  ds_report_achievement where report_date = '2019-06-02';

SELECT * from gb_loan_application_kkd where sign_status = 1 ORDER BY apply_no;



SELECT 	direct_advisor_id as `工号`
	,name as `姓名`
	,dt_month as `统计月份`
	,apply_num as `贷款笔数`
	,apply_amount as `放款金额`
from
(
	SELECT		bbb.direct_advisor_id
		,DATE_FORMAT(review_time,'%Y-%m') as dt_month
		,count(aaa.apply_no) as apply_num
		,sum( review_amount ) as apply_amount
	from
	(
		SELECT * 
		from gb_loan_application_kkd 
		where sign_status = 1 
	) aaa
	inner JOIN
	(
		select * 
		from gb_loan_advisor 
		where direct_advisor_id is not null
	) bbb
	on
		aaa.apply_no = bbb.loan_tid
	GROUP BY 
		bbb.direct_advisor_id
		,DATE_FORMAT(review_time,'%Y-%m')
) ccc
LEFT JOIN
(
	select user_id, name from user where user_id in ( 15170,15171,53,15238,15239,15371 )
) ddd
on ccc.direct_advisor_id = ddd.user_id
ORDER BY
	dt_month
	,direct_advisor_id
;



select * from gb_mcht_blacklist inner join
(
  SELECT tid from gb_mcht_info where mcht_cd in (  '990513059986033','821411359980360','211513050947117')
) aaa
on gb_mcht_blacklist.mcht_tid = aaa.tid
;

SELECT tid from gb_mcht_info where mcht_cd in (  '990513059986033','821411359980360','211513050947117');

SELECT * from gb_mcht_info where mcht_cd in (  '990513059986033','821411359980360','211513050947117');





