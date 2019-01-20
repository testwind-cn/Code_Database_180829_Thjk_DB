select * from (

              select
tid,
uu.user_id as user_id_uu,
um.user_id , 
login_mobile,
mcht_tid,
old_mobile,
open_id,
 uu.create_time as create_time_uu,
um.create_time,
create_type,
device_channel,
uu.is_delete as is_delete_uu,
um.is_delete,
last_login_time,
last_update_source,
uu.last_update_time as last_update_time_uu,
um.last_update_time,
uu.last_update_user_id as last_update_user_id_uu,
um.last_update_user_id,

password,
referrer_mobile,
register_channel,
register_ip_address,
uu.status as status_uu,
um.status,
status_update_time
 from gb_user_mcht um left join gb_user uu on um.user_id = uu.user_id where mcht_tid in
(select mcht_tid from gb_user_mcht group by  mcht_tid having count(*) > 1 )  and um.status = 1 and uu.login_mobile is not null )   hhh
    group by mcht_tid having count(*) > 1 order by mcht_tid,user_id,create_time;

select * from gb_user_mcht where status =1 and is_delete = 0 and mcht_tid != 144446 
# 182163

SELECT * from ( select * from gb_user_mcht where status =1 and is_delete = 0 and mcht_tid != 144446 ) hhh LEFT JOIN gb_user on hhh.user_id = gb_user.user_id

select DISTINCT(mcht_tid) from  ( select * from gb_user_mcht where status =1 and is_delete = 0 and mcht_tid != 144446  ) hhh

select mcht_tid,COUNT(*) from 
( select * from gb_user_mcht where status =1 and is_delete = 0 and mcht_tid != 144446 ) hh GROUP BY mcht_tid HAVING COUNT(*) > 1

select count(*) from gb_user_mcht um left join gb_user u on um.user_id = u.user_id where mcht_tid in (select mcht_tid from gb_user_mcht group by  mcht_tid having count(*) > 1 ) and um.status = 1 order by um.mcht_tid,um.user_id,um.create_time;

SELECT count(*),sum(loan_balance),sum(loan_principal) from s6700 where data_dt = '2018-10-31'



SELECT * from gb_user_mcht where mcht_tid in (144446,
1321918,
1323243,
2428159,
2448570,
2753879)

select * from  gb_user_mcht where user_id in (143544,170445,173133,174151) order by user_id,mcht_tid;




SELECT COUNT(*) from gb_mcht_info
#7519874
SELECT COUNT(DISTINCT tid ) from gb_mcht_info
#7519874







select * from gb_loan_applicaiton_kkd_extra  where is_delete = 0 ;

select *,COUNT(application_tid) from gb_loan_applicaiton_kkd_extra  group by application_tid HAVING COUNT(application_tid) > 1 ;

select *,COUNT(application_tid) from gb_loan_applicaiton_kkd_extra  group by application_tid HAVING COUNT(application_tid) = 1 ;

SELECT *from gb_loan_applicaiton_kkd_extra where application_tid in ( select application_tid from gb_loan_applicaiton_kkd_extra  group by application_tid HAVING COUNT(application_tid) > 1 );


