select c.user_verify_tid, c.legal_name, b.create_time,b.user_id from gb_user_verification b join gb_user_verification_bank_acc c on b.tid =  c.user_verify_tid group by b.user_id having count(*)> 3;



mysql> select count(*),user_id,tid,create_time from gb_user_verification group by user_id having count(*)>3  ;
+----------+---------+--------+---------------------+
| count(*) | user_id | tid    | create_time         |
+----------+---------+--------+---------------------+
|       13 |  162166 | 163715 | 2017-06-22 18:09:25 |
|        4 |  172428 | 175401 | 2017-08-02 16:03:43 |
|        4 |  174256 | 181470 | 2017-09-29 10:54:59 |
|        4 |  175474 | 179716 | 2017-09-11 13:55:31 |
|        5 |  175634 | 182266 | 2017-10-12 11:33:34 |
|        4 |  181278 | 186409 | 2017-11-15 23:38:00 |
|        4 |  183469 | 189247 | 2018-01-04 14:28:39 |
|        5 |  184579 | 190607 | 2018-02-12 11:10:48 |
|        4 |  187529 | 193653 | 2018-05-03 18:48:41 |
|        4 |  188503 | 194598 | 2018-05-15 11:19:25 |
+----------+---------+--------+---------------------+
10 rows in set


mysql> select c.user_verify_tid, c.legal_name, b.create_time,b.user_id from gb_user_verification b join gb_user_verification_bank_acc c on b.tid =  c.user_verify_tid group by b.user_id having count(*)> 3;
+-----------------+------------+---------------------+---------+
| user_verify_tid | legal_name | create_time         | user_id |
+-----------------+------------+---------------------+---------+
|          163715 | 周梅英     | 2017-06-22 18:09:25 |  162166 |
|          175401 | 胡娜       | 2017-08-02 16:03:43 |  172428 |
|          181470 | 孙庆香     | 2017-09-29 10:54:59 |  174256 |
|          179716 | 周效亮     | 2017-09-11 13:55:31 |  175474 |
|          182266 | 杜宁       | 2017-10-12 11:33:34 |  175634 |
|          186409 | 余德芳     | 2017-11-15 23:38:00 |  181278 |
|          189247 | 刘梦       | 2018-01-04 14:28:39 |  183469 |
|          190607 | 白天平     | 2018-02-12 11:10:48 |  184579 |
|          193653 | 周国平     | 2018-05-03 18:48:41 |  187529 |
|          194598 | 张丹丹     | 2018-05-15 11:19:25 |  188503 |
+-----------------+------------+---------------------+---------+
10 rows in set



select a.user_id, a.create_time from gb_user_verification as a where user_id in (select b.user_id from gb_user_verification as b group by b.user_id having count(*) > 3 ) ;

select c.user_verify_tid, c.legal_name,max(c.verify_time) from gb_user_verification_bank_acc as c group by ;







select a.tid,a.user_id, a.create_time, a.status,a.is_delete from gb_user_verification as a where a.user_id in (select b.user_id from gb_user_verification as b group by b.user_id having count(*) > 3 ) and a.is_delete = 0 order by a.user_id;

+--------+---------+---------------------+--------+-----------+
| tid    | user_id | create_time         | status | is_delete |
+--------+---------+---------------------+--------+-----------+
| 193874 |  162166 | 2018-05-09 17:19:19 | 1      |         0 |
| 176013 |  172428 | 2017-08-07 23:49:53 | 1      |         0 |
| 181667 |  174256 | 2017-09-30 16:54:22 | 1      |         0 |
| 179719 |  175474 | 2017-09-11 13:55:31 | 1      |         0 |
| 179720 |  175474 | 2017-09-11 13:55:31 | 1      |         0 |
| 179718 |  175474 | 2017-09-11 13:55:31 | 1      |         0 |
| 179716 |  175474 | 2017-09-11 13:55:31 | 1      |         0 |
| 182310 |  175634 | 2017-10-12 13:44:20 | 1      |         0 |
| 193374 |  181278 | 2018-04-25 21:44:42 | 1      |         0 |
| 189250 |  183469 | 2018-01-04 14:30:01 | 1      |         0 |
| 189249 |  183469 | 2018-01-04 14:29:18 | 1      |         0 |
| 189248 |  183469 | 2018-01-04 14:29:13 | 1      |         0 |
| 189247 |  183469 | 2018-01-04 14:28:39 | 1      |         0 |
| 190653 |  184579 | 2018-02-14 09:28:11 | 1      |         0 |
| 193656 |  187529 | 2018-05-03 18:55:35 | 1      |         0 |
| 194601 |  188503 | 2018-05-15 11:23:19 | 1      |         0 |
+--------+---------+---------------------+--------+-----------+
16 rows in set




select d.max_tid , d.user_id from ( select max(b.tid) as max_tid,b.user_id, b.status,b.is_delete from gb_user_verification as b where b.user_id in (select a.user_id from gb_user_verification as a group by a.user_id having count(*) > 3 ) and b.is_delete = 0 group by b.user_id order by b.user_id ) as d;

+---------+---------+
| max_tid | user_id |
+---------+---------+
|  193874 |  162166 |
|  176013 |  172428 |
|  181667 |  174256 |
|  179720 |  175474 |
|  182310 |  175634 |
|  193374 |  181278 |
|  189250 |  183469 |
|  190653 |  184579 |
|  193656 |  187529 |
|  194601 |  188503 |
+---------+---------+
10 rows in set



select d.max_tid , d.user_id,c.user_verify_tid,c.legal_name,c.legal_id_no,c.legal_id_type from ( select max(b.tid) as max_tid,b.user_id, b.status,b.is_delete from gb_user_verification as b where b.user_id in (select a.user_id from gb_user_verification as a group by a.user_id having count(*) > 3 ) and b.is_delete = 0 group by b.user_id order by b.user_id ) as d left join gb_user_verification_bank_acc as c on d.max_tid = c.user_verify_tid;


+---------+---------+-----------------+------------+--------------------+---------------+
| max_tid | user_id | user_verify_tid | legal_name | legal_id_no        | legal_id_type |
+---------+---------+-----------------+------------+--------------------+---------------+
|  193874 |  162166 |          193874 | 周梅英     | 510225197506192821 | I             |
|  176013 |  172428 |          176013 | 胡娜       | 450204198402290626 | I             |
|  181667 |  174256 |          181667 | 张军元     | 522321198507281218 | I             |
|  179720 |  175474 |          179720 | 周效亮     | 430521198507042599 | I             |
|  182310 |  175634 |          182310 | 杜宁       | 210181198605238011 | I             |
|  193374 |  181278 |          193374 | 余德芳     | 522427198007250021 | I             |
|  189250 |  183469 |          189250 | 刘梦       | 370502198103241641 | I             |
|  190653 |  184579 |          190653 | 白天平     | 232101197207290016 | I             |
|  193656 |  187529 |          193656 | 周国平     | 442527196506055657 | I             |
|  194601 |  188503 |          194601 | 张丹丹     | 410526198803050049 | I             |
+---------+---------+-----------------+------------+--------------------+---------------+
10 rows in set




select dd.max_tid , dd.user_id,cc.user_verify_tid,cc.legal_name,cc.legal_id_no,cc.legal_id_type,ee.address from ( ( select max(bb.tid) as max_tid,bb.user_id, bb.status,bb.is_delete from gb_user_verification as bb where bb.user_id in (select aa.user_id from gb_user_verification as aa group by aa.user_id having count(*) > 3 ) and bb.is_delete = 0 group by bb.user_id order by bb.user_id ) as dd left join gb_user_verification_bank_acc as cc on dd.max_tid = cc.user_verify_tid ) left join gb_user_info as ee on  dd.user_id = ee.user_id;

+---------+---------+-----------------+------------+--------------------+---------------+--------------------------------------------------------+
| max_tid | user_id | user_verify_tid | legal_name | legal_id_no        | legal_id_type | address                                                |
+---------+---------+-----------------+------------+--------------------+---------------+--------------------------------------------------------+
|  193874 |  162166 |          193874 | 周梅英     | 510225197506192821 | I             | 云南省香格里拉市坛城广场以北6一2一4                    |
|  176013 |  172428 |          176013 | 胡娜       | 450204198402290626 | I             | NULL                                                   |
|  181667 |  174256 |          181667 | 张军元     | 522321198507281218 | I             | 贵州省黔西南布依族苗族自治州兴义市桔山镇滴水村五组56号 |
|  179720 |  175474 |          179720 | 周效亮     | 430521198507042599 | I             | NULL                                                   |
|  182310 |  175634 |          182310 | 杜宁       | 210181198605238011 | I             | NULL                                                   |
|  193374 |  181278 |          193374 | 余德芳     | 522427198007250021 | I             | 贵州省威宁县草海镇响塘村万寿宫组                       |
|  189250 |  183469 |          189250 | 刘梦       | 370502198103241641 | I             | 青岛市金华路54号                                       |
|  190653 |  184579 |          190653 | 白天平     | 232101197207290016 | I             | 哈尔滨市南岗区和兴三道街7号                            |
|  193656 |  187529 |          193656 | 周国平     | 442527196506055657 | I             | NULL                                                   |
|  194601 |  188503 |          194601 | 张丹丹     | 410526198803050049 | I             | 河南省滑县滑县道康路五号楼一单元三楼西                 |
+---------+---------+-----------------+------------+--------------------+---------------+--------------------------------------------------------+
10 rows in set



select max(info_id) from gb_user_info where is_delete = 0 group by user_id ;

select edu_level,marital_status,house_status,address,mate_name,mate_id_no from gb_user_info where info_id in ( select max(info_id) from gb_user_info where is_delete = 0 group by user_id  );



select dd.max_tid , dd.user_id,cc.user_verify_tid,cc.legal_name,cc.legal_id_no,cc.legal_id_type,ee.address from ( ( select max(bb.tid) as max_tid,bb.user_id, bb.status,bb.is_delete from gb_user_verification as bb where bb.user_id in (select aa.user_id from gb_user_verification as aa group by aa.user_id having count(*) > 3 ) and bb.is_delete = 0 group by bb.user_id order by bb.user_id ) as dd left join gb_user_verification_bank_acc as cc on dd.max_tid = cc.user_verify_tid ) left join gb_user_info as ee on  dd.user_id = ee.user_id;


( select user_id,edu_level,marital_status,house_status,address,mate_name,mate_id_no from gb_user_info where info_id in ( select max(info_id) from gb_user_info where is_delete = 0 group by user_id  ) ) as ee;




select dd.max_tid , dd.user_id,cc.user_verify_tid,cc.legal_name,cc.legal_id_no,cc.legal_id_type,ee.edu_level,ee.marital_status,ee.house_status,ee.address,ee.mate_name,ee.mate_id_no from ( ( select max(bb.tid) as max_tid,bb.user_id, bb.status,bb.is_delete from gb_user_verification as bb where bb.user_id in (select aa.user_id from gb_user_verification as aa group by aa.user_id having count(*) > 3 ) and bb.is_delete = 0 group by bb.user_id order by bb.user_id ) as dd left join gb_user_verification_bank_acc as cc on dd.max_tid = cc.user_verify_tid ) left join ( select user_id,edu_level,marital_status,house_status,address,mate_name,mate_id_no from gb_user_info where info_id in ( select max(info_id) from gb_user_info where is_delete = 0 group by user_id  ) ) as ee on  dd.user_id = ee.user_id;



+---------+---------+-----------------+------------+--------------------+---------------+-----------+----------------+--------------+--------------------------------------------------------+-----------+--------------------+
| max_tid | user_id | user_verify_tid | legal_name | legal_id_no        | legal_id_type | edu_level | marital_status | house_status | address                                                | mate_name | mate_id_no         |
+---------+---------+-----------------+------------+--------------------+---------------+-----------+----------------+--------------+--------------------------------------------------------+-----------+--------------------+
|  193874 |  162166 |          193874 | 周梅英     | 510225197506192821 | I             | 2         | 2              | 1            | 云南省香格里拉市坛城广场以北6一2一4                    | 万强      | 362502197303202239 |
|  176013 |  172428 |          176013 | 胡娜       | 450204198402290626 | I             | NULL      | NULL           | NULL         | NULL                                                   | NULL      | NULL               |
|  181667 |  174256 |          181667 | 张军元     | 522321198507281218 | I             | 2         | 2              | 1            | 贵州省黔西南布依族苗族自治州兴义市桔山镇滴水村五组56号 | 张倩      | 52232819861125082X |
|  179720 |  175474 |          179720 | 周效亮     | 430521198507042599 | I             | NULL      | NULL           | NULL         | NULL                                                   | NULL      | NULL               |
|  182310 |  175634 |          182310 | 杜宁       | 210181198605238011 | I             | NULL      | NULL           | NULL         | NULL                                                   | NULL      | NULL               |
|  193374 |  181278 |          193374 | 余德芳     | 522427198007250021 | I             | 3         | 3              | 1            | 贵州省威宁县草海镇响塘村万寿宫组                       | NULL      | NULL               |
|  189250 |  183469 |          189250 | 刘梦       | 370502198103241641 | I             | 4         | 2              | 2            | 青岛市金华路54号                                       | 杨瑾      | 370502198010190013 |
|  190653 |  184579 |          190653 | 白天平     | 232101197207290016 | I             | 2         | 2              | 1            | 哈尔滨市南岗区和兴三道街7号                            | 王岩岩    | 370286197705137063 |
|  193656 |  187529 |          193656 | 周国平     | 442527196506055657 | I             | NULL      | NULL           | NULL         | NULL                                                   | NULL      | NULL               |
|  194601 |  188503 |          194601 | 张丹丹     | 410526198803050049 | I             | 3         | 2              | 1            | 河南省滑县滑县道康路五号楼一单元三楼西                 | 徐辉辉    | 41052619871184111  |
+---------+---------+-----------------+------------+--------------------+---------------+-----------+----------------+--------------+--------------------------------------------------------+-----------+--------------------+
10 rows in set

