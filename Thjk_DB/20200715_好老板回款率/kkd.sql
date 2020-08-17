SELECT
		--  data_1.r8                           AS `当月天`,
       data_1.data_dt                       as `报表日期`,

       data_1.q1                            as `全部未结清贷款笔数`,
       data_1.q2                            AS `全部未结清贷款原始本金`,
       data_1.q3                            AS `全部未结清贷款余额-本金`,

       data_1.q4                            as `逾期1天至30天的户数`,
       data_1.q5                            as `逾期1天至30天的贷款原始本金`,
       data_1.q6                            as `逾期1天至30天的贷款余额-本金`,

       data_1.q7                            AS `逾期31天至60天的户数`,
       data_1.q8                            AS `逾期31天至60天的贷款原始本金`,
       data_1.q9                            AS `逾期31天至60天的贷款余额-本金`,

       data_1.q10                           AS `逾期61天至90天的户数`,
       data_1.q11                           AS `逾期61天至90天的贷款原始本金`,
       data_1.q12                           AS `逾期61天至90天的贷款余额-本金`,

       data_1.q13                           AS `逾期90天+的户数`,
       data_1.q14                           AS `逾期90天+的贷款原始本金`,
       data_1.q15                           AS `逾期90天+的贷款余额-本金`
FROM
(
    SELECT
        COUNT(*)                      AS q1,  -- `全部未结清贷款笔数`,
        SUM(`col_10`)                 AS q2,  -- `全部未结清贷款原始本金`,
        SUM(`col_40`)                 AS q3,  -- `全部未结清贷款余额.本金`,

        SUM(IF(`col_47` >= 1 and `col_47` <= 30 , 1, 0))  AS q4,  -- `逾期1天至30天的户数`,
        SUM(
                IF(`col_47` >= 1 and `col_47` <= 30, `col_10`, 0)
            )                         AS q5,  -- `逾期1天至30天的贷款原始本金`,
        SUM(
                IF(`col_47` >= 1 and `col_47` <= 30, `col_40`, 0)
            )                         AS q6,  -- `逾期1天至30天的贷款余额.本金`,


        SUM(IF(`col_47` > 30 and `col_47` <= 60, 1, 0)) AS q7,  -- `逾期31天至60天的户数`,
        SUM(
                IF(`col_47` > 30  and `col_47` <= 60, `col_10`, 0)
            )                         AS q8,  -- `逾期31天至60天的贷款原始本金`,
        SUM(
                IF(`col_47` > 30 and `col_47` <= 60, `col_40`, 0)
            )                         AS q9,  -- `逾期31天至60天的贷款余额.本金`,


        SUM(IF(`col_47` > 60 and `col_47` <= 90, 1, 0)) AS q10,  -- `逾期61天至90天的户数`,
        SUM(
                IF(`col_47` > 60  and `col_47` <= 90, `col_10`, 0)
            )                         AS q11,  -- `逾期61天至90天的贷款原始本金`,
        SUM(
                IF(`col_47` > 60 and `col_47` <= 90, `col_40`, 0)
            )                         AS q12,  -- `逾期61天至90天的贷款余额.本金`,


        SUM(IF(`col_47` > 90, 1, 0)) AS q13, -- `逾期90天+的户数`,
        SUM(
                IF(`col_47` > 90, `col_10`, 0)
            )                         AS q14, -- `逾期90天+的贷款原始本金`,
        SUM(
                IF(`col_47` > 90, `col_40`, 0)
            )                         AS q15, -- `逾期90天+的贷款余额.本金`,


        data_dt,			-- `报表日期`
        DAYOFMONTH(
                DATE_ADD(data_dt, 1)
            )                         AS r8   -- `当月天`
    FROM allinpal_rpt.thbl_rpt_d0009
    WHERE DAYOFMONTH(
               DATE_ADD(data_dt, 1)
           ) = 1
                             -- 每月最后一天
    or data_dt in ( cast( DATE_SUB(CURRENT_DATE, 1) as date) )
                        -- 当天的前一天
                -- or data_dt in ( '2019-03-28','2019-03-21','2019-04-03','2019-09-12')
                -- 以前做过这几天
                or data_dt in (
                        cast( DATE_SUB('2019-09-12',0) as date)
                        ,cast( DATE_SUB('2019-11-18',0) as date)
                        ,cast( DATE_SUB('2020-06-30',0) as date)

                    )
    GROUP BY data_dt
) data_1