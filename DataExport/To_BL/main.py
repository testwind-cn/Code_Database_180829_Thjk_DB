#!/usr/bin/ python
# -*- coding: utf-8 -*-

"""
保理数据核验：信审
"""

import pandas as pd
import pymysql

'''
http://www.runoob.com/python3/python3-mysql.html
打开数据库连接
db = pymysql.connect("10.91.1.10", "root", "RiskControl@2018", "unify")

使用 cursor() 方法创建一个游标对象 cursor
cursor = db.cursor()

使用 execute()  方法执行 SQL 查询
cursor.execute("SELECT VERSION()")

使用 fetchone() 方法获取单条数据.
data = cursor.fetchone()

print("Database version : %s " % data)

关闭数据库连
db.close()
sql = "select * from bak_190122_credit_pos_flow"
df = pd.read_sql_query(sql, con=db)
a = df.count()
print(a)
df2 = df.sample(n=20)
a = df2.count()
print(a)
'''

db = pymysql.connect("10.91.1.10", "root", "RiskControl@2018", "unify")

# 信审历史表
sql = "SELECT distinct(sss.merchant_ap) from ( select * from bak_190122_credit_history where prod_code=1 )  as sss"
df1 = pd.read_sql_query(sql, con=db)
a = df1.count()
print(a)

# 信审流水表
sql = "select distinct(merchant_ap) from bak_190122_credit_pos_flow"
df2 = pd.read_sql_query(sql, con=db)
a = df2.count()
print(a)

# 信审风险表
sql = "select distinct(merchant_ap) from bak_190122_credit_pos_risk"
df3 = pd.read_sql_query(sql, con=db)
a = df3.count()
print(a)


k_df1 = pd.merge(df1, df2, how='inner', on='merchant_ap')
a = k_df1.count()
print(a)
k_df1 = k_df1.sample(n=200)


k_df2 = pd.merge(df3, df2, how='inner', on='merchant_ap')
a = k_df2.count()
print(a)
k_df2 = k_df2.sample(n=200)

##########################
# 信审历史表 k_df1 = 1 * 2

sql = "select * from bak_190122_credit_history where prod_code=1 "
df1 = pd.read_sql_query(sql, con=db)
a = df1.count()
print(a)

f_df1 = pd.merge(df1, k_df1, how='inner', on='merchant_ap')
a = f_df1.count()
print(a)

writer = pd.ExcelWriter('output1.xlsx')
f_df1.to_excel(writer, 'Sheet1')
writer.save()

##########################
# 信审流水表  k_df1 = 1 * 2 ，  k_df2 = 2 * 3
sql = "select * from bak_190122_credit_pos_flow"
df2 = pd.read_sql_query(sql, con=db)
a = df2.count()
print(a)

f_df2 = pd.merge(df2, k_df1, how='inner', on='merchant_ap')
a = f_df2.count()
print(a)

writer = pd.ExcelWriter('output2-1.xlsx')
f_df2.to_excel(writer, 'Sheet1')
writer.save()

f_df2 = pd.merge(df2, k_df2, how='inner', on='merchant_ap')
a = f_df2.count()
print(a)

writer = pd.ExcelWriter('output2-2.xlsx')
f_df2.to_excel(writer, 'Sheet1')
writer.save()

#########################
# 信审风险表  k_df2 = 2 * 3
sql = "select * from bak_190122_credit_pos_risk"
df3 = pd.read_sql_query(sql, con=db)
a = df3.count()
print(a)

f_df3 = pd.merge(df3, k_df2, how='inner', on='merchant_ap')
a = f_df3.count()
print(a)

writer = pd.ExcelWriter('output3.xlsx')
f_df3.to_excel(writer, 'Sheet1')
writer.save()
