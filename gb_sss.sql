

select concat( left(mcht_name,4),'***') as '地区', concat( left(legal_name,1),'**') as '姓名', concat( left(legal_id_no,6),'************') as '身份证号码',mcht_cd as '商户号',concat( left(bank_mobile_phone,7),'****') as '手机号码',loan_principal as '放款金额' , period as '贷款周期',
prd_desc as '还款方式',loan_period_num as '贷款期数',loan_balance as '贷款余额',loan_date as '贷款起息日',loan_maturity_date as '贷款到期日',overdue_period as '贷款逾期天数（期数）',
status as '贷款状态',curr_period as '当前期数',overdue_days as '逾期天数' from

(
select hhh.* ,userinfo.* from ( select * from s6700 where data_dt = '2018-10-31' ) hhh left join gb_mcht_info on hhh.mcht_cd = gb_mcht_info.mcht_cd
left join ( select * from gb_user_mcht where gb_user_mcht.is_delete = 0 and gb_user_mcht.`status` = 1 ) gum on gb_mcht_info.tid = gum.mcht_tid
left join
(
SELECT * from
(
select
#  gb_user_verification.tid as tid,
  gb_user_verification.user_id as user_id,
  gb_user_verification_bank_acc.legal_name as legal_name,
  gb_user_verification_bank_acc.legal_id_no as legal_id_no,
#  gb_user_verification.verify_type as verify_type,
  gb_user_verification_bank_acc.mobile_phone as bank_mobile_phone,
#  gb_user_verification.status as status,
  IFNULL(gb_user_verification.create_time,str_to_date('1970-01-01 00:00:00','%Y-%m-%d %T' ) ) as create_time
#  IFNULL(gb_user_verification.last_update_time,str_to_date('1970-01-01 00:00:00','%Y-%m-%d %T' ) ) as last_update_time,
#  gb_user_verification.is_delete as is_delete
from
  gb_user_verification
left join
  gb_user_verification_bank_acc
on gb_user_verification.tid = gb_user_verification_bank_acc.user_verify_tid
where
		 gb_user_verification.is_delete =  0 and gb_user_verification.status = 1
order by user_id,create_time desc
) b GROUP BY
    b.user_id
) userinfo
on gum.user_id = userinfo.user_id
) www order by loan_date desc