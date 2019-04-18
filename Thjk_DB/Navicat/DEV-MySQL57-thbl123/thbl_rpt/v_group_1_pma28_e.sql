SELECT `dim_table`.`date_v`                                                                  AS `date_v`,
       (sum(`data_table`.`c01`) / f_get_ma_len(`dim_table`.`date_v`, '2015-08-03', 1, 28.0)) AS `sum(全部结清-笔数) / 28`,
       (sum(`data_table`.`c02`) / f_get_ma_len(`dim_table`.`date_v`, '2015-08-03', 1, 28.0)) AS `sum(全部结清-原始贷款本金) / 28`,
       (sum(`data_table`.`c06`) / f_get_ma_len(`dim_table`.`date_v`, '2015-08-03', 1, 28.0)) AS `sum(全部未结清-笔数) / 28`,
       (sum(`data_table`.`c07`) / f_get_ma_len(`dim_table`.`date_v`, '2015-08-03', 1, 28.0)) AS `sum(全部未结清-原始贷款本金) / 28`,
       (sum(`data_table`.`c08`) / f_get_ma_len(`dim_table`.`date_v`, '2015-08-03', 1, 28.0)) AS `sum(逾期30天+-笔数) / 28`,
       (sum(`data_table`.`c09`) / f_get_ma_len(`dim_table`.`date_v`, '2015-08-03', 1, 28.0)) AS `sum(逾期30天+原始贷款本金) / 28`,
       (sum(`data_table`.`c10`) / f_get_ma_len(`dim_table`.`date_v`, '2015-08-03', 1, 28.0)) AS `sum(逾期30天+贷款余额.本金) / 28`,
       (sum(`data_table`.`c11`) / f_get_ma_len(`dim_table`.`date_v`, '2015-08-03', 1, 28.0)) AS `sum(逾期90天+-笔数) / 28`,
       (sum(`data_table`.`c12`) / f_get_ma_len(`dim_table`.`date_v`, '2015-08-03', 1, 28.0)) AS `sum(逾期90天+原始贷款本金) / 28`,
       (sum(`data_table`.`c13`) / f_get_ma_len(`dim_table`.`date_v`, '2015-08-03', 1, 28.0)) AS `sum(逾期90天+贷款余额.本金) / 28`
FROM (
         SELECT `dim_date`.`date_v` AS `date_v`
         FROM `dim_date`
         WHERE (
                       (`dim_date`.`date_v` >= str_to_date('2015-08-05', '%Y-%m-%d'))
                       AND (`dim_date`.`date_v` <= str_to_date('2019-05-01', '%Y-%m-%d'))
                   )
         ORDER BY `dim_date`.`date_v`
     ) `dim_table`
         LEFT JOIN (
    SELECT `v_group_1`.`tgroup` AS `tgroup`,
           `v_group_1`.`c01`    AS `c01`,
           `v_group_1`.`c02`    AS `c02`,
           `v_group_1`.`c06`    AS `c06`,
           `v_group_1`.`c07`    AS `c07`,
           `v_group_1`.`c08`    AS `c08`,
           `v_group_1`.`c09`    AS `c09`,
           `v_group_1`.`c10`    AS `c10`,
           `v_group_1`.`c11`    AS `c11`,
           `v_group_1`.`c12`    AS `c12`,
           `v_group_1`.`c13`    AS `c13`
    FROM `v_group_1`
    ORDER BY `v_group_1`.`tgroup`
) `data_table` ON (`f_date_ck_e`(`dim_table`.`date_v`, `data_table`.`tgroup`, 1, 28))
GROUP BY `dim_table`.`date_v`
ORDER BY `dim_table`.`date_v`