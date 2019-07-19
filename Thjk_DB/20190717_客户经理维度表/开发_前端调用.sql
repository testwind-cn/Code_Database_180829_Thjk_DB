-- 橘子快跑前端输入：
--      客户经理账号： 【email】
--      开始日期：     【start_date】
--      结束日期：     【end_date】

-- 研发代码中间查询获取变量：
--      客户经理ID     【manager_id】=
select manager_id from dim_code_manager where manager_code='[email]' and platform_code=2;

-- 查询客户经理统计数据：
select
    s_credit_amt,
    s_credit_num,
    s_use_amt,
    s_use_num,
    ba_product.product_name,
    dim_identity_manager.name,
    dim_identity_manager.mobile
from (
    select
        sum(credit_amt) s_credit_amt,
        sum(credit_num) s_credit_num,
        sum(use_amt)    s_use_amt,
        sum(use_num)    s_use_num,
        manager_id,
        product_code
    from dm_manager_statistic
    where
        manager_id  = '【manager_id】' and data_dt >= '[start_date]'  and data_dt <= '[end_date]'
    group by
        product_code
) sum_statistic
left join ba_product
    on sum_statistic.product_code = ba_product.product_code
left join dim_identity_manager
    on sum_statistic.manager_id = dim_identity_manager.manager_id;
