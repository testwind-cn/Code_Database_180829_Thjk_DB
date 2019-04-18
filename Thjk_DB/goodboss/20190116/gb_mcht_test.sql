-- 贷款最大逾期天数
SELECT db_no, max(overdue_days) as max_overdue_days from s6700 where data_dt < "2019-1-1 0:00:00" GROUP BY db_no


-- 被拒绝的贷款申请
SELECT * from gb_loan_application_kkd where apply_status in (1,4,12) and apply_time <= "2018-9-1 0:00:00" and user_credit_report_history_tid > 0


-- 分店数量
SELECT DISTINCT( concat( mcht_cd,'-',branch_cd) ) from gb_branch_info;
-- 保理 2019-1-22 22:00 查询   1774259
-- 保理 2019-1-23 09:00 查询   1774300

--商户数量
SELECT DISTINCT( mcht_cd ) from gb_mcht_info;
-- 保理 2019-1-23 09:50 查询  7741614 行
-- 通金 2019-1-22   导出文件  7713456 行