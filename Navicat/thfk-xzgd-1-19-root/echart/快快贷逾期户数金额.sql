SELECT data_1.data_dt,
       data_1.r1                           AS `逾期30天的户数`,
       data_1.r2                           AS `逾期30天的贷款余额.本金`,
       data_1.r3                           AS `逾期90天的户数`,
       data_1.r4                           AS `逾期90天的贷款余额.本金`,
       data_1.r5                           AS `逾期30天的贷款原始本金`,
       data_1.r6                           AS `逾期90天的贷款原始本金`,
       data_1.r7                           AS `未结清贷款原始本金`,
       data_1.r8                           AS `当月天`,
       data_2.r1                           AS `已结清贷款笔数`,
       data_2.r2                           AS `已结清贷款原始本金`,
       data_1.r7 + data_2.r2               AS `累计放款原始本金`,
       data_1.r2 / (data_1.r7 + data_2.r2) AS `30天逾期贷款余额本金 / 累计放款原始本金`,
       data_1.r4 / (data_1.r7 + data_2.r2) AS `90天逾期贷款余额本金 / 累计放款原始本金`
FROM (
         SELECT COUNT(*)                      AS num,
                SUM(IF(`col_47` >= 30, 1, 0)) AS r1, -- `逾期30天的户数`,
                SUM(
                        IF(`col_47` >= 30, `col_40`, 0)
                    )                         AS r2, -- `逾期30天的贷款余额.本金`,
                SUM(IF(`col_47` >= 90, 1, 0)) AS r3, -- `逾期90天的户数`,
                SUM(
                        IF(`col_47` >= 90, `col_40`, 0)
                    )                         AS r4, -- `逾期90天的贷款余额.本金`,
                SUM(
                        IF(`col_47` >= 30, `col_10`, 0)
                    )                         AS r5, -- `逾期30天的贷款原始本金`,
                SUM(
                        IF(`col_47` >= 90, `col_10`, 0)
                    )                         AS r6, -- `逾期90天的贷款原始本金`,
                SUM(`col_10`)                 AS r7, -- `未结清贷款原始本金`,
                data_dt,
                DAYOFMONTH(
                        DATE_ADD(data_dt, INTERVAL (1) DAY)
                    )                         AS r8  -- `当月天`
         FROM d0009
         WHERE DAYOFMONTH(
                       DATE_ADD(data_dt, INTERVAL (1) DAY)
                   ) = 1
-- or data_dt in ( '2019-03-28','2019-03-21','2019-04-03')
         GROUP BY data_dt
     ) data_1
         LEFT JOIN
     (
         SELECT data_dt,
                count(*)      as r1, -- `已结清贷款笔数`
                sum(`col_10`) AS r2  -- `已结清贷款原始本金`
         FROM d0010
         WHERE DAYOFMONTH(
                       DATE_ADD(data_dt, INTERVAL (1) DAY)
                   ) = 1
-- or data_dt in ( '2019-03-28','2019-03-21','2019-04-03')
         GROUP BY data_dt
     ) data_2
     ON data_1.data_dt = data_2.data_dt