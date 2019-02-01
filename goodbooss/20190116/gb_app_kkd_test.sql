SELECT * from gb_loan_application_kkd where sign_status =1 and apply_status =15;

SELECT * from gb_loan_application_kkd where sign_status =1 and apply_status =15;
-- sign_status =1 , apply_status = 1, 一条
-- sign_status =1 , apply_status = 11, 4698条       11 - 申请完成|签约完成
-- sign_status =1 , apply_status = 12, 一条
-- sign_status =1 , apply_status = 14, 298条                14 - 贷款终止
-- sign_status =1 , apply_status = 15, 12352条                15 - 贷款完成

SELECT DISTINCT(loan_cashed_status) from gb_loan_application_kkd where apply_status =17;
SELECT DISTINCT(ft_status) from gb_loan_application_kkd where apply_status =17;
SELECT DISTINCT(loan_cashed_status) from gb_loan_application_kkd where apply_status =17;
-- review_loan_audit_status = null , ft_status=1,2. ,loan_cashed_status=null


SELECT * from gb_loan_application_kkd where apply_status =1 and loan_cashed_status='L05';
--  1条    1 - 预审拒绝       L05 - 放款成功

SELECT DISTINCT(review_loan_audit_status) from gb_loan_application_kkd where apply_status =1;

 SELECT * from gb_loan_application_kkd where apply_status =1 and review_loan_audit_status='L05';
--  1条      1 - 预审拒绝       L05 - 放款成功

SELECT * from gb_loan_application_kkd where apply_status =1 and review_loan_audit_status='L04';
--  1条      1 - 预审拒绝            L04 - 审批拒绝
-- 他的 loan_cashed_status = null

SELECT * from gb_loan_application_kkd where apply_status =1 and loan_cashed_status='L05';
-- 1 条

SELECT DISTINCT(review_loan_audit_status) from gb_loan_application_kkd where apply_status =4;
-- null, L05, L02

SELECT * from gb_loan_application_kkd where apply_status =4 and review_loan_audit_status='L05;'
-- 有8条，都没有放款， loan_audit_status = null ,sign_status =0

SELECT * from gb_loan_application_kkd where apply_status =4 and review_loan_audit_status='L02';
-- 1 条

SELECT DISTINCT(review_loan_audit_status) from gb_loan_application_kkd where apply_status =12;
--    review_loan_audit_status =    L04 - 审批拒绝       L08 - 校验失败

SELECT DISTINCT(loan_cashed_status) from gb_loan_application_kkd where apply_status =12;
--  loan_cashed_status = null