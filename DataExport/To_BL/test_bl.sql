select distinct(merchant_ap) as r_ap from bak_190122_credit_pos_risk join

( select distinct(merchant_ap) as f_ap from bak_190122_credit_pos_flow ) as sss
join
( select distinct(merchant_ap) as h_ap from bak_190122_credit_history ) as hhh
on
bak_190122_credit_pos_risk.merchant_ap = sss.f_ap
and bak_190122_credit_pos_risk.merchant_ap = hhh.h_ap;

-- -----------------------------------------

SELECT distinct(sss.merchant_ap) from ( select * from bak_190122_credit_history where prod_code=1 )  as sss;

-- -----------------------------------------

select distinct(merchant_ap) as r_ap from bak_190122_entry_loan_use where bak_190122_entry_loan_use.prod_code = 1;

-- -----------------------------------------

select distinct(merchant_ap) as r_ap from bak_190122_entry_loan_use join

( select distinct(merchant_ap) as f_ap from bak_190122_entry_merchant ) as sss
on
bak_190122_entry_loan_use.merchant_ap = sss.f_ap  where bak_190122_entry_loan_use.prod_code = 1;

-- -----------------------------------------

