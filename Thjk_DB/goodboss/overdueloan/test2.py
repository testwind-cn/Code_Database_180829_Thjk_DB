from impala.dbapi import connect
conn = connect(host='192.168.90.13', port=10000, database='default',user='hive',auth_mechanism='PLAIN')
cur = conn.cursor()
cur.execute('show databases')
print(cur.fetchall()) 
cur.execute('use report_dw')
cur.execute('show tables')
print(cur.fetchall())
cur.execute('show create table syb_trx_record ')
print(cur.fetchall())
cur.execute('select * from report_dw.syb_trx_record limit 10')
print(cur.fetchall())
