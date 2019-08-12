

with
aaa as (
    SELECT email, 
        user_email, 
        phone,
        cert_no,
        name
    from rds_rc.t_operation_emp
    where t_operation_emp.`ARCHIVE_FLAG` = 1 and t_operation_emp.`STATUS` in ('1','2','9')
)
select
    aaa.*,
    bbb.*
from
aaa
left join
aaa bbb
on
    -- aaa.user_email = bbb.user_email and    -- 邮箱相同的
    -- length(regexp_replace(aaa.user_email,'\\s','')) > 5 -- and upper(regexp_replace(aaa.user_email,'\\s','') ) <> 'NULL'

    -- aaa.cert_no = bbb.cert_no and        -- 身份证相同的
    -- length(regexp_replace(aaa.cert_no,'\\s','')) > 5 -- and upper(regexp_replace(aaa.cert_no,'\\s','') ) <> 'NULL'
    
    -- aaa.phone = bbb.phone and        -- 不存在手机号码相同的
    -- length(regexp_replace(aaa.phone,'\\s','')) > 5 -- and upper(regexp_replace(aaa.phone,'\\s','') ) <> 'NULL'

    -- aaa.email = bbb.phone and            -- 账号 和 手机号 相同的
    -- length(regexp_replace(aaa.email,'\\s','')) > 5 -- and upper(regexp_replace(aaa.email,'\\s','') ) <> 'NULL'
    
where
    aaa.email <> bbb.email  -- 代表  bbb.email is not null
order by aaa.email desc
;












with
aaa as (
    SELECT email, 
        user_email, 
        phone,
        cert_no,
        name
    from rds_rc.t_operation_emp
    where t_operation_emp.`ARCHIVE_FLAG` = 1 and t_operation_emp.`STATUS` in ('1','2','9')
)
select
    aaa.*,
    bbb.*
from
aaa
left join
aaa bbb
on
    -- 一. 姓名相同、身份证相同
    
    -- 1、姓名相同
    regexp_replace(aaa.name,'\\s','') = regexp_replace(bbb.name,'\\s','')  
    and length(regexp_replace(aaa.name,'\\s','')) >= 2 and upper(regexp_replace(aaa.name,'\\s','')) <> 'NULL'
    
    -- 2、身份证相同    
    and upper(regexp_replace(aaa.cert_no,'\\s','')) = upper(regexp_replace(bbb.cert_no,'\\s',''))
    and length(regexp_replace(aaa.cert_no,'\\s','')) >= 5  -- 代表  身份证 is not null
where
    -- 0、不能是自己和自己交叉
    aaa.email <> bbb.email  -- 也代表  bbb.email is not null
order by aaa.email desc
;





    
    


with
aaa as (
    SELECT email, 
        user_email, 
        phone,
        cert_no,
        name
    from rds_rc.t_operation_emp
    where t_operation_emp.`ARCHIVE_FLAG` = 1 and t_operation_emp.`STATUS` in ('1','2','9')
)
select
    aaa.*,
    bbb.*
from
aaa
left join
aaa bbb
on
    -- 二. aaa.name=bbb.name, and aaa.iden和bbb.iden至少一个为空,  and bbb.user_email=bbb.user_email
    
    -- 1、姓名相同
    regexp_replace(aaa.name,'\\s','') = regexp_replace(bbb.name,'\\s','')  
    and length(regexp_replace(aaa.name,'\\s','')) >= 2 and upper(regexp_replace(aaa.name,'\\s','')) <> 'NULL'
    
    -- 2、user_email 相同
    and upper(regexp_replace(aaa.user_email,'\\s','')) = upper(regexp_replace(bbb.user_email,'\\s',''))  
    and length(regexp_replace(aaa.user_email,'\\s','')) >= 5  -- 代表  aaa.user_email is not null
where
    -- 0、不能是自己和自己交叉
    aaa.email <> bbb.email  -- 也代表  bbb.email is not null
    and ( -- 3、身份证至少一个为空
        upper(regexp_replace(coalesce(aaa.cert_no,'NULL'),'\\s','')) = 'NULL'
     or upper(regexp_replace(coalesce(bbb.cert_no,'NULL'),'\\s','')) = 'NULL'
     ) 
order by aaa.email desc
;







with
aaa as (
    SELECT email, 
        user_email, 
        phone,
        cert_no,
        name
    from rds_rc.t_operation_emp
    where t_operation_emp.`ARCHIVE_FLAG` = 1 and t_operation_emp.`STATUS` in ('1','2','9')
)
select
    aaa.*,
    bbb.*
from
aaa
left join
aaa bbb
on
    -- 三. aaa.name=bbb.name,   aaa.iden至少一个为空 , user_email<>user_email ,  aaa.email=bbb.phone
    
    -- 1、姓名相同
    regexp_replace(aaa.name,'\\s','') = regexp_replace(bbb.name,'\\s','')  
    and length(regexp_replace(aaa.name,'\\s','')) >= 2 and upper(regexp_replace(aaa.name,'\\s','')) <> 'NULL'
    
    -- 2、aaa.email 和 bbb.phone 相同
    and regexp_replace(aaa.email,'\\s','') = regexp_replace(bbb.phone,'\\s','')  
    and length(regexp_replace(aaa.email,'\\s','')) >=5  -- 代表  aaa.email is not null
where
    -- 0、不能是自己和自己交叉
    aaa.email <> bbb.email  -- 也代表  bbb.email is not null
    and -- 3、身份证至少一个为空
    (     upper(regexp_replace(coalesce(aaa.cert_no,'NULL'),'\\s','')) = 'NULL'
     or upper(regexp_replace(coalesce(bbb.cert_no,'NULL'),'\\s','')) = 'NULL'
    )
    and    -- 4、user_email 不相等，或者至少一个为空
    (    upper(regexp_replace(aaa.user_email,'\\s','')) <> upper(regexp_replace(bbb.user_email,'\\s',''))  
     or upper(regexp_replace(coalesce(aaa.user_email,'NULL'),'\\s','')) = 'NULL'
     or upper(regexp_replace(coalesce(bbb.user_email,'NULL'),'\\s','')) = 'NULL'
    )
order by aaa.email desc
;




/*

-- 创建个人代码表
CREATE TABLE IF NOT EXISTS dim.dim_identity_manager (
    `manager_id` int COMMENT '身份证表ID',
    `cert_no`  string COMMENT '身份证',
    `name`  string COMMENT '姓名',
    `phone`  string COMMENT '电话',
    `email`  string COMMENT '邮箱'
)
 ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\"",
    "escapeChar" = "\\"
)
STORED AS TEXTFILE;


DROP TABLE IF EXISTS dim.tmp_dim_identity_manager;
-- 创建个人代码临时表
CREATE TABLE IF NOT EXISTS dim.tmp_dim_identity_manager (
    `manager_id` int COMMENT '身份证表ID',
    `cert_no`  string COMMENT '身份证',
    `name`  string COMMENT '姓名',
    `phone`  string COMMENT '电话',
    `email`  string COMMENT '邮箱'
)
 ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\"",
    "escapeChar" = "\\"
)
STORED AS TEXTFILE;





left join
(
    select
        coalesce(max(cast(manager_id as int)),0)    t_id_max
    from dim.dim_identity_manager
) ttt2

*/





select
    first_value(ggg.s_platform_code) OVER( partition by ggg.platform_code,ggg.manager_code order by s_manager_code) as f_platform_code  -- 希望是 order by s_platform_code,s_manager_code 但不支持
    , first_value(ggg.s_manager_code) OVER( partition by ggg.platform_code,ggg.manager_code order by s_manager_code) as f_manager_code  -- 希望是 order by s_platform_code,s_manager_code 但不支持
    , first_value(ggg.s_cert_no) OVER( partition by ggg.platform_code,ggg.manager_code order by length(ggg.s_cert_no) desc) as f_cert_no
    , first_value(ggg.s_name) OVER( partition by ggg.platform_code,ggg.manager_code order by length(ggg.s_name) desc) as f_name
    , first_value(ggg.s_phone) OVER( partition by ggg.platform_code,ggg.manager_code order by length(ggg.s_phone) desc) as f_phone
    , first_value(ggg.s_email) OVER( partition by ggg.platform_code,ggg.manager_code order by length(ggg.s_email) desc) as f_email
    , ggg.platform_code
    , ggg.manager_code
    , ggg.cert_no
    , ggg.name
    , ggg.phone
    , ggg.email
    , first_value(ggg.manager_id) OVER( partition by ggg.platform_code,ggg.manager_code order by ggg.manager_id desc) as manager_id
from ggg
order by f_platform_code,f_manager_code desc
; -- 然后还要根据 platform_code， manager_code 分组聚合
/*
2    18937776366    41132619880813281X    李政雨    18825388067    18825388067@163.com    2    18937776366    41132619880813281X    李政雨    18937776366    18825388067@163.com
2    18937776366    41132619880813281X    李政雨    18937776366    18825388067@163.com    2    lizy4    41132619880813281X    李政雨    18825388067    lizy4@allinpay.com
2    18909518355    640203198308031010    魏明    18195153180    weiming@allinpay.com    2    weiming    640203198308031010    魏明    18909518355    weiming@allinpay.com
2    18909518355    640203198308031010    魏明    18909518355    weiming@allinpay.com    2    18909518355    640203198308031010    魏明    18195153180    null
2    18608981667    460033197807123912    陈太雷    18889889199    chentl1@allinpay.com    2    18608981667    460033197807123912    陈太雷    18608981667    null
2    18608981667    460033197807123912    陈太雷    18608981667    chentl1@allinpay.com    2    chentl1    460033197807123912    陈太雷    18889889199    chentl1@allinpay.com
2    18605800633    330902198201021014    钟鑫    18605800633    zhongxin@allinpay.com    2    zhongxin    330902198201021014    钟鑫    17328876058    zhongxin@allinpay.com
2    18605800633    330902198201021014    钟鑫    17328876058    zhongxin@allinpay.com    2    18605800633    330902198201021014    钟鑫    18605800633    42207394@qq.com
2    18559753007    350212198711203537    林盛华    18559753007    linsh@allinpay.com    2    linsh    NULL    林盛华    15105985935    linsh@allinpay.com
2    18559753007    350212198711203537    林盛华    15105985935    linsh@allinpay.com    2    18559753007    350212198711203537    林盛华    18559753007    linsh@allinpay.com
2    18538069922    410108198810200071    范会颖    18538069922    fanhy1@allinpay.com    2    fanhy1    NULL    范会颖    18137865344    fanhy1@allinpay.com
2    18538069922    410108198810200071    范会颖    18137865344    fanhy1@allinpay.com    2    18538069922    410108198810200071    范会颖    18538069922    fanhy1@allinpay.com
2    18193252807    620422198908060858    石伟栋    18193252807    shiwd@allinpay.com    2    shiwd    620422198908060858    石伟栋    13209327835    shiwd@allinpay.com
2    18193252807    620422198908060858    石伟栋    13209327835    shiwd@allinpay.com    2    18193252807    620422198908060858    石伟栋    18193252807    shiwd@allinpay.com
2    17797180464    63012119931006361X    尼玛丹珍    13997229110    nimadz@allinpay.com    2    nimadz    NULL    尼玛丹珍    17797180464    nimadz@allinpay.com
2    17797180464    63012119931006361X    尼玛丹珍    17797180464    nimadz@allinpay.com    2    17797180464    63012119931006361X    尼玛丹珍    13997229110    619814000@qq.com
2    17735139553    142724199208101912    张国威    18734827797    zhanggw1@allinpay.com    2    17735139553    142724199208101912    张国威    17735139553    381273132@qq.com
2    17735139553    142724199208101912    张国威    17735139553    zhanggw1@allinpay.com    2    zhanggw1    142724199208101912    张国威    18734827797    zhanggw1@allinpay.com
2    15928290123    510521198505030754    严维刚    15928290123    yanwg@allinpay.com    2    yanwg    510521198505030754    严维刚    18109022362    yanwg@allinpay.com
2    15928290123    510521198505030754    严维刚    18109022362    yanwg@allinpay.com    2    15928290123    510521198505030754    严维刚    15928290123    yanwg@allinpay.com
2    15804677750    230303198506154918    王润哲    15804677750    251198398@qq.com    2    wangrz    230303198506154918    王润哲    13846058150    89018419@qq.com
2    15804677750    230303198506154918    王润哲    13846058150    251198398@qq.com    2    15804677750    230303198506154918    王润哲    15804677750    251198398@qq.com
2    15657330933    330402199008050013    程家骏    17328876019    chengjj@allinpay.com    2    chengjj    NULL    程家骏    15657330933    chengjj@allinpay.com
2    15657330933    330402199008050013    程家骏    15657330933    chengjj@allinpay.com    2    15657330933    330402199008050013    程家骏    17328876019    null
2    15305377773    370881198206221119    颜瑞    18266845660    13863749187@163.com    2    yanrui    NULL    颜瑞    15305377773    yanrui@allinpay.com
2    15305377773    370881198206221119    颜瑞    15305377773    yanrui@allinpay.com    2    15305377773    370881198206221119    颜瑞    18266845660    13863749187@163.com
2    13998101221    210502198812210341    王紫薇    13624006681    wangzw5@allinpay.com    2    13998101221    210502198812210341    王紫薇    13998101221    wangzw5@allinpay.com
2    13998101221    210502198812210341    王紫薇    13998101221    wangzw5@allinpay.com    2    wangzw5    NULL    王紫薇    13624006681    wangzw5@allinpay.com
2    13863841515    370685198809010032    张乃中    13255509000    zhangnz@allinpay.com    2    zhangnz    NULL    张乃中    13863841515    zhangnz@allinpay.com
2    13863841515    370685198809010032    张乃中    13863841515    zhangnz@allinpay.com    2    13863841515    370685198809010032    张乃中    13255509000    398190751@qq.com
2    13701991037    NULL    杨可木    13700000000    yangkm@allinpay.com    2    yangkm    NULL    杨可木    13701991037    yangkm@allinpay.com
2    13701991037    NULL    杨可木    13701991037    yangkm@allinpay.com    2    13701991037    NULL    杨可木    13700000000    yangkm@allinpay.com
2    13653601127    142703199005060936    马肖博    17535156127    maxb@allinpay.com    2    maxb    142703199005060936    马肖博    13653601127    maxb@allinpay.com
2    13653601127    142703199005060936    马肖博    13653601127    maxb@allinpay.com    2    13653601127    142703199005060936    马肖博    17535156127    maxb@allinpay.com
2    13605765577    332625197501211173    刘井贵    13605765577    767901043@qq.com    2    767901043@qq.com    NULL    刘井贵    13357600326    767901043@qq.com
2    13605765577    332625197501211173    刘井贵    13357600326    767901043@qq.com    2    13605765577    332625197501211173    刘井贵    13605765577    767901043@qq.com
2    13235900808    352225199101191039    陈骏杭    13235900808    chenjh@allinpay.com    2    chenjh    NULL    陈骏杭    13685030784    chenjh@allinpay.com
2    13235900808    352225199101191039    陈骏杭    13685030784    chenjh@allinpay.com    2    13235900808    352225199101191039    陈骏杭    13235900808    chenjh@allinpay.com
2    13193226841    500237199704186775    谭远舜    17830136659    2421780976@qq.com    2    13193226841    500237199704186775    谭远舜    13193226841    2421780976@qq.com
2    13193226841    500237199704186775    谭远舜    13193226841    2421780976@qq.com    2    17830136659    500237199704186775    谭远舜    17830136659    null
2    13114969725    120103199103183544    陈蕾    13114969725    3207566553@qq.com    2    3207566553@qq.com    NULL    陈蕾    13114960725    3207566553@qq.com
2    13114969725    120103199103183544    陈蕾    13114960725    3207566553@qq.com    2    13114969725    120103199103183544    陈蕾    13114969725    3207566553@qq.com
2    1041175494@qq.com    510421199709100047    蔡蔓蔓    18109022335    1041175494@qq.com    2    15181261790    510421199709100047    蔡蔓蔓    15181261790    1041175494@qq.com
2    1041175494@qq.com    510421199709100047    蔡蔓蔓    15181261790    1041175494@qq.com    2    1041175494@qq.com    NULL    蔡蔓蔓    18109022335    1041175494@qq.com
*/






/*
-- BEGIN 最新和历史客户经理融合后，计算出可能是一个人名下的多个客户经理编码
select * from ggg
order by ggg.s_platform_code, ccc.s_manager_code desc
2    18937776366    41132619880813281X    李政雨    18825388067    18825388067@163.com    2    18937776366    41132619880813281X    李政雨    18937776366    18825388067@163.com
2    18937776366    41132619880813281X    李政雨    18937776366    18825388067@163.com    2    lizy4    41132619880813281X    李政雨    18825388067    lizy4@allinpay.com
2    18909518355    640203198308031010    魏明    18909518355    weiming@allinpay.com    2    18909518355    640203198308031010    魏明    18195153180    null
2    18909518355    640203198308031010    魏明    18195153180    weiming@allinpay.com    2    weiming    640203198308031010    魏明    18909518355    weiming@allinpay.com
2    18608981667    460033197807123912    陈太雷    18608981667    chentl1@allinpay.com    2    chentl1    460033197807123912    陈太雷    18889889199    chentl1@allinpay.com
2    18608981667    460033197807123912    陈太雷    18889889199    chentl1@allinpay.com    2    18608981667    460033197807123912    陈太雷    18608981667    null
2    18605800633    330902198201021014    钟鑫    17328876058    zhongxin@allinpay.com    2    18605800633    330902198201021014    钟鑫    18605800633    42207394@qq.com
2    18605800633    330902198201021014    钟鑫    18605800633    zhongxin@allinpay.com    2    zhongxin    330902198201021014    钟鑫    17328876058    zhongxin@allinpay.com
2    18559753007    350212198711203537    林盛华    15105985935    linsh@allinpay.com    2    18559753007    350212198711203537    林盛华    18559753007    linsh@allinpay.com
2    18559753007    350212198711203537    林盛华    18559753007    linsh@allinpay.com    2    linsh    NULL    林盛华    15105985935    linsh@allinpay.com
2    18538069922    410108198810200071    范会颖    18137865344    fanhy1@allinpay.com    2    18538069922    410108198810200071    范会颖    18538069922    fanhy1@allinpay.com
2    18538069922    410108198810200071    范会颖    18538069922    fanhy1@allinpay.com    2    fanhy1    NULL    范会颖    18137865344    fanhy1@allinpay.com
2    18193252807    620422198908060858    石伟栋    18193252807    shiwd@allinpay.com    2    shiwd    620422198908060858    石伟栋    13209327835    shiwd@allinpay.com
2    18193252807    620422198908060858    石伟栋    13209327835    shiwd@allinpay.com    2    18193252807    620422198908060858    石伟栋    18193252807    shiwd@allinpay.com
2    17797180464    63012119931006361X    尼玛丹珍    17797180464    nimadz@allinpay.com    2    17797180464    63012119931006361X    尼玛丹珍    13997229110    619814000@qq.com
2    17797180464    63012119931006361X    尼玛丹珍    13997229110    nimadz@allinpay.com    2    nimadz    NULL    尼玛丹珍    17797180464    nimadz@allinpay.com
2    17735139553    142724199208101912    张国威    17735139553    zhanggw1@allinpay.com    2    zhanggw1    142724199208101912    张国威    18734827797    zhanggw1@allinpay.com
2    17735139553    142724199208101912    张国威    18734827797    zhanggw1@allinpay.com    2    17735139553    142724199208101912    张国威    17735139553    381273132@qq.com
2    15928290123    510521198505030754    严维刚    15928290123    yanwg@allinpay.com    2    yanwg    510521198505030754    严维刚    18109022362    yanwg@allinpay.com
2    15928290123    510521198505030754    严维刚    18109022362    yanwg@allinpay.com    2    15928290123    510521198505030754    严维刚    15928290123    yanwg@allinpay.com
2    15804677750    230303198506154918    王润哲    13846058150    251198398@qq.com    2    15804677750    230303198506154918    王润哲    15804677750    251198398@qq.com
2    15804677750    230303198506154918    王润哲    15804677750    251198398@qq.com    2    wangrz    230303198506154918    王润哲    13846058150    89018419@qq.com
2    15657330933    330402199008050013    程家骏    15657330933    chengjj@allinpay.com    2    15657330933    330402199008050013    程家骏    17328876019    null
2    15657330933    330402199008050013    程家骏    17328876019    chengjj@allinpay.com    2    chengjj    NULL    程家骏    15657330933    chengjj@allinpay.com
2    15305377773    370881198206221119    颜瑞    15305377773    yanrui@allinpay.com    2    15305377773    370881198206221119    颜瑞    18266845660    13863749187@163.com
2    15305377773    370881198206221119    颜瑞    18266845660    13863749187@163.com    2    yanrui    NULL    颜瑞    15305377773    yanrui@allinpay.com
2    13998101221    210502198812210341    王紫薇    13624006681    wangzw5@allinpay.com    2    13998101221    210502198812210341    王紫薇    13998101221    wangzw5@allinpay.com
2    13998101221    210502198812210341    王紫薇    13998101221    wangzw5@allinpay.com    2    wangzw5    NULL    王紫薇    13624006681    wangzw5@allinpay.com
2    13863841515    370685198809010032    张乃中    13863841515    zhangnz@allinpay.com    2    13863841515    370685198809010032    张乃中    13255509000    398190751@qq.com
2    13863841515    370685198809010032    张乃中    13255509000    zhangnz@allinpay.com    2    zhangnz    NULL    张乃中    13863841515    zhangnz@allinpay.com
2    13701991037    NULL    杨可木    13700000000    yangkm@allinpay.com    2    yangkm    NULL    杨可木    13701991037    yangkm@allinpay.com
2    13701991037    NULL    杨可木    13701991037    yangkm@allinpay.com    2    13701991037    NULL    杨可木    13700000000    yangkm@allinpay.com
2    13653601127    142703199005060936    马肖博    13653601127    maxb@allinpay.com    2    13653601127    142703199005060936    马肖博    17535156127    maxb@allinpay.com
2    13653601127    142703199005060936    马肖博    17535156127    maxb@allinpay.com    2    maxb    142703199005060936    马肖博    13653601127    maxb@allinpay.com
2    13605765577    332625197501211173    刘井贵    13357600326    767901043@qq.com    2    13605765577    332625197501211173    刘井贵    13605765577    767901043@qq.com
2    13605765577    332625197501211173    刘井贵    13605765577    767901043@qq.com    2    767901043@qq.com    NULL    刘井贵    13357600326    767901043@qq.com
2    13235900808    352225199101191039    陈骏杭    13685030784    chenjh@allinpay.com    2    13235900808    352225199101191039    陈骏杭    13235900808    chenjh@allinpay.com
2    13235900808    352225199101191039    陈骏杭    13235900808    chenjh@allinpay.com    2    chenjh    NULL    陈骏杭    13685030784    chenjh@allinpay.com
2    13193226841    500237199704186775    谭远舜    17830136659    2421780976@qq.com    2    13193226841    500237199704186775    谭远舜    13193226841    2421780976@qq.com
2    13193226841    500237199704186775    谭远舜    13193226841    2421780976@qq.com    2    17830136659    500237199704186775    谭远舜    17830136659    null
2    13114969725    120103199103183544    陈蕾    13114969725    3207566553@qq.com    2    3207566553@qq.com    NULL    陈蕾    13114960725    3207566553@qq.com
2    13114969725    120103199103183544    陈蕾    13114960725    3207566553@qq.com    2    13114969725    120103199103183544    陈蕾    13114969725    3207566553@qq.com
2    1041175494@qq.com    510421199709100047    蔡蔓蔓    18109022335    1041175494@qq.com    2    15181261790    510421199709100047    蔡蔓蔓    15181261790    1041175494@qq.com
2    1041175494@qq.com    510421199709100047    蔡蔓蔓    15181261790    1041175494@qq.com    2    1041175494@qq.com    NULL    蔡蔓蔓    18109022335    1041175494@qq.com
*/



add jar MaxStrUDAF.jar;

create temporary function maxstr as 'com.wind.hive.MaxStrUDAF';

SELECT 1 as rol;

SELECT explode(array('asdsdsdsa','dsd','ZZZZsdsdsaasd')) as dt;


select
    maxstr(ccc.dt), ccc.col
from (
    select
        aaa.dt, bbb.col
    from (
        SELECT explode(array('asdsdsdsa','dsd','ZZZZsdsdsaasd','xcxc','我的时间在PPPDSADSDA','ssszz')) as dt
    ) aaa
   join (
       SELECT 1 as col
    ) bbb
) ccc
group by ccc.col;






