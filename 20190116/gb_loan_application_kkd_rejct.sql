-- 拒绝的贷款申请用户 2019-1-16

SELECT
	gb_loan_application_kkd.mcht_tid,
	gb_mcht_info.tid,
	gb_loan_application_kkd.bank_account,
	gua.bank_account,
	bank_account_name,
	gua.legal_name,
	user_credit_report_history_tid ,

	gb_user.login_mobile,
	MD5(gb_user.login_mobile) as login_mobile_md5,
	gb_mcht_info.mcht_cd,
	concat( '*' ,right( if(LENGTH(bank_account_name)>0,bank_account_name,gua.legal_name),1)) as username,
	gua.legal_id_no,
	MD5(gua.legal_id_no) as legal_id_no_md5,
	due_bill_no,
	loan_start_date,
	loan_end_date,
  review_amount,
  review_duration,
  review_payment_type ,
	apply_time,
	apply_status,
	sign_status,
	sign_time,
	review_loan_audit_status,
	loan_cashed_status,
	apply_source,
	apply_type ,
	edu_level,
	marital_status,
	house_status,
	b_days.max_overdue_days

from
	gb_loan_application_kkd


LEFT JOIN 
	( SELECT * FROM gb_user_mcht where `status` = 1 and is_delete =  0  )
	as gum
	on gb_loan_application_kkd.mcht_tid = gum.mcht_tid  

LEFT JOIN
(
		SELECT
			gb_user_verification.user_id,
			gb_user_verification_bank_acc.legal_id_no,
			gb_user_verification_bank_acc.legal_name,
			gb_user_verification_bank_acc.mobile_phone,
			gb_user_verification_bank_acc.bank_account,
			gb_user_verification.create_time
		FROM gb_user_verification
		LEFT JOIN
			gb_user_verification_bank_acc
		on
			gb_user_verification.tid = gb_user_verification_bank_acc.user_verify_tid
		where
			gb_user_verification.status = 1 and gb_user_verification.is_delete =  0 and
			gb_user_verification_bank_acc.status = 1 and gb_user_verification_bank_acc.is_delete =  0
			and gb_user_verification.tid  in
				( # 认证超过一次的有,69 条
					SELECT bbb.tid from
					( # 必须用 select 包裹一下 , 里面的 result 是 186492, 但工作效果是 186561 (和 group by之前一样 )
						SELECT aaa.tid from
						(
							SELECT * from gb_user_verification where
									gb_user_verification.status = 1 and gb_user_verification.is_delete =  0
							ORDER BY gb_user_verification.create_time  desc
						) as aaa
						GROUP BY aaa.user_id
					) as bbb  # 必须用 select 包裹一下 , 里面的 result 是 186492, 但工作效果是 186561 (和 group by之前一样 )
				)

	) as gua 
	on gum.user_id = gua.user_id  




LEFT JOIN
  gb_user 
	on gua.user_id  = gb_user.user_id


LEFT JOIN
      -- 找出这个借据号的最大逾期天数
	( SELECT db_no, max(overdue_days) as max_overdue_days from s6700 GROUP BY db_no )
	as b_days
	on gb_loan_application_kkd.due_bill_no = b_days.db_no

left join
	gb_mcht_info
	on gb_mcht_info.tid = gb_loan_application_kkd.mcht_tid


where apply_status in ( 1,4,12) and user_credit_report_history_tid >0
and apply_time > '2017-6-1 00:00:00' and apply_time < '2018-12-1 00:00:00'


AND

gb_loan_application_kkd.mcht_tid not in
-- 不是在成功贷款申请用户集合中，就是说这个用户从来都没申请成功过
( SELECT mcht_tid from  gb_loan_application_kkd

where review_loan_audit_status = 'L05' and loan_cashed_status in ('L05','L07','L99') and user_credit_report_history_tid >0 )
