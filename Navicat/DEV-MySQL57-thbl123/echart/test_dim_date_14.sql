SELECT
	to_days( '2015-08-03' ) AS `to_days( '2015-08-03' )`,
	`f_date_normal_b` ( `dim_date`.`date_v`, '2015-08-03', 14 ) AS `aaa1`,
	`f_date_normal_e` ( `dim_date`.`date_v`, '2015-08-03', 14 ) AS `bbb1`,
	(
	'2015-08-03' + INTERVAL (
	(
	(
	( ( to_days( `dim_date`.`date_v` ) - to_days( '2015-08-03' ) ) + ( ( 736178 DIV 14 ) * 14 ) ) DIV 14 
	) * 14 
	) - ( ( 736178 DIV 14 ) * 14 ) 
	) DAY 
	) AS `date_v1`,
	(
	'2015-08-03' + INTERVAL (
	(
	(
	(
	(
	( ( to_days( `dim_date`.`date_v` ) - to_days( '2015-08-03' ) ) + ( ( 736178 DIV 14 ) * 14 ) ) DIV 14 
	) * 14 
	) + 14 
	) - ( ( 736178 DIV 14 ) * 14 ) 
	) - 1 
	) DAY 
	) AS `tgroup2`,
	`dim_date`.`date_v` AS `date_v` 
FROM
	`dim_date` 
WHERE
	( ( `dim_date`.`date_v` >= str_to_date( '2014-01-05', '%Y-%m-%d' ) ) AND ( `dim_date`.`date_v` <= str_to_date( '2020-05-01', '%Y-%m-%d' ) ) )