-- 查看 d0009, d0010, s6700 里的数据量

SELECT data_dt,count(*) from d0009 GROUP BY data_dt ORDER BY data_dt desc;
SELECT data_dt,count(*) from d0010 GROUP BY data_dt ORDER BY data_dt desc;
SELECT data_dt,count(*) from s6700 GROUP BY data_dt ORDER BY data_dt desc;