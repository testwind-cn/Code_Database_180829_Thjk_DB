SELECT
	date_v, f_date_normal_e(date_v,'2015-08-03',14),  f_get_ma_len(date_v,'2015-08-03',14,5),
	f_get_ma_len(`date_v`,'2015-08-03',1,14.0)
FROM
dim_date