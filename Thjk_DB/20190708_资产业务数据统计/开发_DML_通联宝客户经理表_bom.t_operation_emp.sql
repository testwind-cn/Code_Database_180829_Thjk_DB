-- 6313条，没有重复，正常数据（未归档删除），包括：正常、停用、注销
SELECT
    count(*) nnn,
    email
from (
    SELECT
        *
    from bom.t_operation_emp
    where
        bom.t_operation_emp.`ARCHIVE_FLAG` = 1
        and
        bom.t_operation_emp.`STATUS` in ('1','2','9')
) aaa
GROUP BY email
ORDER BY nnn desc;




-- 6401条，不重复的账号数量
SELECT DISTINCT(bom.t_operation_emp.email)
from bom.t_operation_emp;




-- 无效数据：新建没通过的，被归档的（删除的）
-- 156条记录，DISTINCT(t_operation_emp.email) = 88个账号
SELECT
    t_operation_emp.`STATUS`,
    CASE t_operation_emp.`STATUS`
        WHEN 'A' THEN '新建待审核'
        WHEN 'B' THEN '停用待审核'
        WHEN 'C' THEN '启用待审核'
        WHEN 'Z' THEN '注销待审核'
        WHEN '0' THEN '审核未通过'
        WHEN '1' THEN '正常'
        WHEN '2' THEN '停用'
        WHEN '9' THEN '注销'
        ELSE '其他'
    END as n_s,
    CASE t_operation_emp.`ARCHIVE_FLAG`
        WHEN 2 THEN '归档'
        WHEN 1 THEN '正常'
        ELSE '其他'
    END as n_s2,
    t_operation_emp.name, t_operation_emp.ARCHIVE_FLAG,
    t_operation_emp.*
from bom.t_operation_emp
LEFT JOIN (
    SELECT
        email
    from t_operation_emp
    where
        t_operation_emp.`ARCHIVE_FLAG` = 1
        and
        t_operation_emp.`STATUS` in ('1','2','9')
) aaa
on t_operation_emp.email = aaa.email
where aaa.email is null
ORDER BY t_operation_emp.email;


-- 未归档的记录，同一个email有2条及以上的，
-- 将这些 email 的全部记录取出，有147条，研究它的数据停用启用过程
SELECT
    t_operation_emp.`STATUS`,
    CASE t_operation_emp.`STATUS`
        WHEN 'A' THEN '新建待审核'
        WHEN 'B' THEN '停用待审核'
        WHEN 'C' THEN '启用待审核'
        WHEN 'Z' THEN '注销待审核'
        WHEN '0' THEN '审核未通过'
        WHEN '1' THEN '正常'
        WHEN '2' THEN '停用'
        WHEN '9' THEN '注销'
        ELSE '其他'
    END as n_s,
    CASE t_operation_emp.`ARCHIVE_FLAG`
        WHEN 2 THEN '归档'
        WHEN 1 THEN '正常'
        ELSE '其他'
    END as n_s2,
    t_operation_emp.name, t_operation_emp.ARCHIVE_FLAG,
    FROM_UNIXTIME(t_operation_emp.CREATE_TIME/1000,'%Y-%m-%d %H:%i:%s') '创建时间',
    FROM_UNIXTIME(t_operation_emp.LAST_MODIFY_TIME/1000,'%Y-%m-%d %H:%i:%s') '修改时间',
    t_operation_emp.RECORD_NO,
    t_operation_emp.L_RECORD_NO,
    bbb.nnn, bbb.email,
    t_operation_emp.*
from `t_operation_emp`
LEFT JOIN (
    SELECT count(*) nnn ,email
    from (
        SELECT * from `t_operation_emp` where ARCHIVE_FLAG=1 ORDER BY email
    ) aaa
    GROUP BY email
    having nnn>1
) bbb
on t_operation_emp.email = bbb.email
where
    bbb.email is not null
    -- and ARCHIVE_FLAG=1
ORDER BY t_operation_emp.email, t_operation_emp.CREATE_TIME
;
