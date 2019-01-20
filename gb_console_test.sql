SELECT g.mcht_tid, g.loan_amount, g.review_loan_audit_status,
  m.legal_name, m.legal_cert_no
from gb_loan_application_kkd g LEFT JOIN merchant m
 on g.mcht_tid = m.merchant_bl where review_loan_audit_status='L05' and legal_cert_no is null;





select * from gb_user_mcht a where (a.user_id,a.mcht_tid) in (
    select user_id ,mcht_tid  from gb_user_mcht group by  user_id ,mcht_tid having count(*)>1 );
#### 11 行数据, 相同 user_id, mcht_tid 有4个
select *,count(*) from gb_user_mcht group by  user_id ,mcht_tid having count(*)>1;
# 143544	144446	4
# 170445	2448570	2
# 173133	2753879	2
# 174151	1321918	3

#### 13=11+2 行数据, 相同 user_id, mcht_tid ,有两个是正常的
select * from  gb_user_mcht where user_id in (143544,170445,173133,174151) order by user_id,mcht_tid;

select * from gb_user  where user_id = 142985;

select count(*) from gb_user_mcht;
# 193720  # 总条目数
select count(*) from gb_user_mcht where user_id is not null;
# 193720  # user_id非空总条目数
select count(*) from gb_user_mcht where mcht_tid is not null;
# 193720  # mcht_tid非空总条目数
select count(*) from ( select distinct(user_id) from gb_user_mcht ) sss;
# 181268  # 唯一的用户号
select count(*) from ( select distinct(mcht_tid) from gb_user_mcht ) sss;
# 184950  # 唯一的商户号


# ==== 考虑用户
select count(*) from gb_user_mcht where user_id in (select user_id from gb_user_mcht group by  user_id having count(*) = 1 ) order by user_id,mcht_tid,create_time;
# 172528  # 172528 +21192 = 193720
select *        from gb_user_mcht where user_id in (select user_id from gb_user_mcht group by  user_id having count(*) > 1 ) order by user_id,mcht_tid,create_time;
select count(*) from gb_user_mcht where user_id in (select user_id from gb_user_mcht group by  user_id having count(*) > 1 ) order by user_id,mcht_tid,create_time;
# 21192
select count(*) from (select user_id from gb_user_mcht group by  user_id having count(*) > 1) hhh;
# 8740   ,8740 +172528 = 181268,  21192 条其实就 8740 个
# ==== 考虑用户


# ==== 考虑商户
select count(*) from gb_user_mcht where mcht_tid in (select mcht_tid from gb_user_mcht group by  mcht_tid having count(*) = 1 ) order by user_id,mcht_tid,create_time;
# 176273   #  176273 + 17447  = 193720
select *        from gb_user_mcht where mcht_tid in (select mcht_tid from gb_user_mcht group by  mcht_tid having count(*) > 1 ) order by mcht_tid,user_id,create_time;
select count(*) from gb_user_mcht where mcht_tid in (select mcht_tid from gb_user_mcht group by  mcht_tid having count(*) > 1 ) order by user_id,mcht_tid,create_time;
# 17447
select count(*) from (select mcht_tid from gb_user_mcht group by  mcht_tid having count(*) > 1 ) hhh;
# 8677 , # 8677 +176273 = 184950,  17447 条其实就 8677 个




select distinct mcht_tid  from gb_user_mcht where status = 1  and is_delete = 0 and mcht_tid != 144446 ;
# 182245 -- 正确
select distinct user_id   from gb_user_mcht where status = 1  and is_delete = 0 and mcht_tid != 144446 ;
# 171946

select mcht_tid ,count(*)from gb_user_mcht where status = 1  and is_delete = 1  group by  mcht_tid having count(*) > 1;
#     有8212个有and is_delete = 1，错误
select * from gb_user_mcht where status = 1 and mcht_tid in ( select mcht_tid from gb_user_mcht where status = 1 group by  mcht_tid having count(*) > 1 ) ;
# 16431
select * from gb_user_mcht where                 mcht_tid in ( select mcht_tid from gb_user_mcht where status = 1 group by  mcht_tid having count(*) > 1 )  order by mcht_tid,user_id,create_time;
# 16510 ---错误

select *        from gb_user_mcht um left join gb_user u on um.user_id = u.user_id where um.status = 1 and um.is_delete = 0 and mcht_tid != 144446 and u.is_delete = 0 and u.login_mobile is not null order by um.mcht_tid,um.user_id,um.create_time;
# 405552 ---错误



# 商户用户关系，不要在on条件加下面，因为有的用户已经改手机号：and u.is_delete = 0 and u.login_mobile is not null
select u.user_id        from gb_user_mcht um left join gb_user u on um.user_id = u.user_id where um.status = 1 and um.is_delete = 0 and mcht_tid != 144446 order by um.mcht_tid,um.user_id,um.create_time;
# 182245 -- 正确，这个是结果
### ？？？？？？？
###############  按年龄  ##############################
select count(*) as all_sum_no, sum(ss.sum_amount) as all_sum_amount, ss.age
from (select count(*)                                            as sum_no,
             sum(loan_amount)                                    as sum_amount,
             legal_name,
             legal_id_no,
             year(curdate()) - substring(legal_id_no, 7, 4)      as age,
             cast(substring(legal_id_no, 17, 1) as UNSIGNED) % 2 as gender
      from (select glak.mcht_tid,
                   glak.loan_amount,
                   glak.review_loan_audit_status,
                   gum.legal_name,
                   gum.legal_id_no,
                   gum.mcht_cd
            from gb_loan_application_kkd glak
                     left join (select um.user_id,
                                       gvbac.legal_name,
                                       gvbac.legal_id_no,
                                       um.mcht_tid,
                                       gb_mcht_info.mcht_cd
                                from gb_user_mcht um
                                         left join gb_user u on um.user_id = u.user_id
                                         left join (select legal_id_no, legal_name, guv.user_id
                                                    from gb_user_verification_bank_acc guvba
                                                             left join (select *
                                                                        from (select *
                                                                        #把认证成功的银行卡按人分组，按时间逆序，取最新的一个
                                                                              from gb_user_verification
                                                                              order by user_id asc, create_time desc) hh
                                                                        where status = 1
                                                                        group by user_id) guv
                                                             on guvba.user_verify_tid = guv.tid
                                                    where guv.status = 1) gvbac on gvbac.user_id = um.user_id
                                         left join gb_mcht_info on gb_mcht_info.tid = um.mcht_tid
                                where um.status = 1
                                  and um.is_delete = 0
                                  and mcht_tid != 144446
# order by um.mcht_tid, um.user_id, um.create_time;
                               ) gum on gum.mcht_tid = glak.mcht_tid
            where review_loan_audit_status = 'L05') rr
      GROUP BY rr.legal_id_no) ss
GROUP BY ss.age;
### ？？？？？？？

#### A plan
select legal_id_no, legal_name, guv.user_id,guvba.user_verify_tid
from gb_user_verification_bank_acc guvba
         left join (select *
                    from (select *
                        #把认证成功的银行卡按人分组，按时间逆序，取最新的一个
                          from gb_user_verification
                          order by user_id asc, create_time desc) hh
                    where status = 1
                    group by user_id) guv on guvba.user_verify_tid = guv.tid
where guv.status = 1

#### A plan
select *, legal_id_no, legal_name, guv.user_id, guvba.user_verify_tid
from gb_user_verification_bank_acc guvba
         left join (select *
                    from (select *
                          from gb_user_verification
                        /*  where user_id in (select user_id
                                            from gb_user_verification guv
                                            where guv.status = 1
                                            group by guv.user_id
                                            having count(guv.user_id) > 1)*/
                          order by user_id asc, create_time desc) hh
                    where status = 1
                    group by user_id) guv on guvba.user_verify_tid = guv.tid
where guv.status = 1

#### B plan
select *
from (select guvba.*, guv.user_id #,guvba.legal_id_no, legal_name,  guvba.user_verify_tid
      from gb_user_verification_bank_acc guvba
               left join gb_user_verification guv on guvba.user_verify_tid = guv.tid
      where guv.status = 1
    /*    and user_id in (select user_id
                        from gb_user_verification guv
                        where guv.status = 1
                        group by guv.user_id
                        having count(guv.user_id) > 1)*/
      order by user_id asc, guv.create_time desc) hh
where status = 1
group by user_id;




#############################################################
select legal_id_no,legal_name,guv.user_id from gb_user_verification_bank_acc guvba left join gb_user_verification guv on guvba.user_verify_tid = guv.tid where guv.status = 1
# 185819  #不重复的银行卡认证，有675个重复认证的

select distinct guv.user_id from gb_user_verification_bank_acc guvba left join gb_user_verification guv on guvba.user_verify_tid = guv.tid where guv.status = 1
# 185144 #不重复的银行卡认证

select * from
    ( select * from gb_user_verification where user_id in ( select user_id from  gb_user_verification guv where guv.status = 1 group by guv.user_id having count(guv.user_id) > 1 ) order by user_id asc,create_time desc ) hh
   group by user_id
# 分组后就 598个 ； 1273个银行卡认证，有一半是老账户自动认证，分组后就 598个

select * from
    ( select * from gb_user_verification order by user_id asc,create_time desc ) hh
where status = 1 group by user_id
# 185144 #不重复的银行卡认证，有675个重复认证的

select  *  from gb_user_verification_bank_acc ;# 185819个银行卡认证
select  *  from gb_user_verification ; # 185819个银行卡认证

select  *,legal_id_no, legal_name,guv.user_id from gb_user_verification_bank_acc guvba left join gb_user_verification guv on guvba.user_verify_tid = guv.tid where guv.status = 1 group by guv.user_id;
# 185144 #不重复的银行卡认证
select  legal_id_no, legal_name,guv.user_id from gb_user_verification_bank_acc guvba left join
    ( select * from
        ( select * from gb_user_verification order by user_id asc,create_time desc ) hh
      where status = 1 group by user_id ) guv
 on guvba.user_verify_tid = guv.tid where guv.status = 1;
# 185144 #不重复的银行卡认证 #这个是结果
#############################################################




select *        from gb_user_mcht where mcht_tid in (select mcht_tid from gb_user_mcht group by  mcht_tid having count(*) > 1 ) and status = 1 order by mcht_tid,user_id,create_time;
select *        from gb_user_mcht um left join gb_user u on um.user_id = u.user_id where mcht_tid in (select mcht_tid from gb_user_mcht group by  mcht_tid having count(*) > 1 ) and um.status = 1 and u.login_mobile is not null order by um.mcht_tid,um.user_id,um.create_time;
select count(*) from gb_user_mcht um left join gb_user u on um.user_id = u.user_id where mcht_tid in (select mcht_tid from gb_user_mcht group by  mcht_tid having count(*) > 1 ) and um.status = 1 and u.login_mobile is not null order by um.mcht_tid,um.user_id,um.create_time;
# 8357
select * from (

              select uu.create_time as create_time_uu,
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
login_mobile,
mcht_tid,
old_mobile,
open_id,
password,
referrer_mobile,
register_channel,
register_ip_address,
uu.status as status_uu,
um.status,
status_update_time,
tid,
uu.user_id as user_id_uu,
um.user_id       from gb_user_mcht um left join gb_user uu on um.user_id = uu.user_id where mcht_tid in
(select mcht_tid from gb_user_mcht group by  mcht_tid having count(*) > 1 )  and um.status = 1 and uu.login_mobile is not null )   hhh
    group by mcht_tid having count(*) > 1 order by mcht_tid,user_id,create_time;










select * from gb_loan_application_kkd where review_loan_audit_status='L05'  limit 100;
select mcht_tid,loan_amount,review_loan_audit_status from gb_loan_application_kkd where review_loan_audit_status='L05'  limit 100;


select * from gb_mcht_info where gb_mcht_info.tid=1369278;

select * from merchant_info where merchant_info.mcht_cd='821150251370485';
select legal_person_name,legal_person_id_no from merchant_info where merchant_info.mcht_cd='821150251370485';

###################
select max(c),count(*) from ( select count(*) as c from merchant_info group by mcht_cd ) ccc;
# 1, 7483589
select count(*) as c from merchant_info;
#7 483589
###################

select mcht_tid,loan_amount,review_loan_audit_status,mmm.tid,mmm.mcht_cd,iii.legal_person_name,legal_person_id_no from gb_loan_application_kkd aaa
    LEFT JOIN gb_mcht_info mmm on aaa.mcht_tid = mmm.tid
    LEFT JOIN merchant_info iii on mmm.mcht_cd = iii.mcht_cd
where aaa.review_loan_audit_status='L05' and mcht_tid in
    (1726102,1961756,1907143,1907143,8443091,8430190,2294961,8915766,8388993,9011534,8910558);


select mcht_tid,loan_amount,review_loan_audit_status,mmm.tid,mmm.mcht_cd,iii.legal_person_name,legal_person_id_no from gb_loan_application_kkd aaa
    LEFT JOIN gb_mcht_info mmm on aaa.mcht_tid = mmm.tid
    LEFT JOIN merchant_info iii on mmm.mcht_cd = iii.mcht_cd
where aaa.review_loan_audit_status='L05';






######  test
select count(*) from user;
select count(*) from gb_user;

select * from gb_user where user_id
in (62896,123731,53173,63004,53550,65480,65320,49566,65311,52150);


select tid,user_id,create_time from gb_user_verification where user_id
in (62896,123731,53173,63004,53550,65480,65320,49566,65311,52150);

select * from gb_user_verification where user_id
in (62896,123731,53173,63004,53550,65480,65320,49566,65311,52150);

select * from gb_user_verification_bank_acc where user_verify_tid
in (49566,52150,53173,53550,62896,63004,65311,65320,65480,123731);




select * from gb_loan_applicaiton_kkd_extra;