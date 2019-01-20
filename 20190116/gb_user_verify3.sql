-- 2019-1-16 身份证号码

SELECT *
FROM gb_user_verification
       LEFT JOIN gb_user_verification_bank_acc
         on gb_user_verification.tid = gb_user_verification_bank_acc.user_verify_tid
where gb_user_verification.status = 1
  and gb_user_verification.is_delete = 0
  and gb_user_verification_bank_acc.status = 1
  and gb_user_verification_bank_acc.is_delete = 0
  and gb_user_verification.user_id in
      (SELECT gb_user_verification.user_id
       FROM gb_user_verification
              LEFT JOIN gb_user_verification_bank_acc
                on gb_user_verification.tid = gb_user_verification_bank_acc.user_verify_tid
       where gb_user_verification.status = 1
         and gb_user_verification.is_delete = 0
         and gb_user_verification_bank_acc.status = 1
         and gb_user_verification_bank_acc.is_delete = 0
         and gb_user_verification.tid not in
                        -- 认证超过一次的有,69 条
             (SELECT bbb.tid
             -- # 必须用 select 包裹一下 , 里面的 result 是 186492, 但工作效果是 186561 (和 group by之前一样 )
              from (SELECT aaa.tid
                    from (SELECT *
                          from gb_user_verification
                          where gb_user_verification.status = 1
                            and gb_user_verification.is_delete = 0
                          ORDER BY gb_user_verification.create_time desc
                         ) as aaa
                    GROUP BY aaa.user_id
                   ) as bbb
             -- 必须用 select 包裹一下 , 里面的 result 是 186492, 但工作效果是 186561 [和 group by之前一样 ]
             )
      )
order by user_id