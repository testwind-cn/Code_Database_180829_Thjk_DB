#!/usr/bin/ python
# -*- coding: utf-8 -*-

"""
保理数据核验：入口
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
sql = "select * from credit_pos_flow"
df = pd.read_sql_query(sql, con=db)
a = df.count()
print(a)
df2 = df.sample(n=20)
a = df2.count()
print(a)
'''

db = pymysql.connect("10.91.1.10", "root", "RiskControl@2018", "data_warehouse")


sql = "SELECT distinct(sss.merchant_ap) from ( select * from entry_loan_apply where prod_code=1 )  as sss"
df4 = pd.read_sql_query(sql, con=db)
a = df4.count()
print(a)

sql = "SELECT distinct(sss.merchant_ap) from ( select * from entry_loan_use where prod_code=1 )  as sss"
df5 = pd.read_sql_query(sql, con=db)
a = df5.count()
print(a)

sql = "select distinct(merchant_ap) from entry_merchant"
df6 = pd.read_sql_query(sql, con=db)
a = df6.count()
print(a)

sql = "select distinct(merchant_ap) from entry_pos_flow"
df7 = pd.read_sql_query(sql, con=db)
a = df7.count()
print(a)

#  k_df1 = 5 * 6
k_df1 = pd.merge(df5, df6, how='inner', on='merchant_ap')
a = k_df1.count()
print(a)
k_df1 = k_df1.sample(n=20)

#  k_df2 = 4 * 6
k_df2 = pd.merge(df4, df6, how='inner', on='merchant_ap')
a = k_df2.count()
print(a)
k_df2 = k_df2.sample(n=20)

##########################
# 4 入口贷款表   k_df1 = 5 * 6 ，  k_df2 = 4 * 6

sql = "select * from entry_loan_apply where prod_code=1 "
df4 = pd.read_sql_query(sql, con=db)
a = df4.count()
print(a)

f_df4 = pd.merge(df4, k_df2, how='inner', on='merchant_ap')
a = f_df4.count()
print(a)

writer = pd.ExcelWriter('output4.xlsx')
f_df4.to_excel(writer, 'Sheet1')
writer.save()

# f_df4 = pd.merge(df4,k_df1,how='inner',on='merchant_ap')
# a = f_df4.count()
# print(a)

# writer = pd.ExcelWriter('output4-2.xlsx')
# f_df4.to_excel(writer, 'Sheet1')
# writer.save()

##########################
# 5 入口支用表  k_df1 = 5 * 6 ，  k_df2 = 4 * 6
sql = "select * from entry_loan_use where prod_code=1 "
df5 = pd.read_sql_query(sql, con=db)
a = df5.count()
print(a)

f_df5 = pd.merge(df5, k_df2, how='inner', on='merchant_ap')
a = f_df5.count()
print(a)

writer = pd.ExcelWriter('output5.xlsx')
f_df5.to_excel(writer, 'Sheet1')
writer.save()

##########################
# 6 入口商户表   k_df1 = 5 * 6 ，  k_df2 = 4 * 6

sql = "select * from entry_merchant"
df6 = pd.read_sql_query(sql, con=db)
a = df6.count()
print(a)

f_df6 = pd.merge(df6, k_df2, how='inner', on='merchant_ap')
a = f_df6.count()
print(a)

writer = pd.ExcelWriter('output6-1.xlsx')
f_df6.to_excel(writer, 'Sheet1')
writer.save()

# f_df6 = pd.merge(df6,k_df1,how='inner',on='merchant_ap')
# a = f_df6.count()
# print(a)

# writer = pd.ExcelWriter('output6-2.xlsx')
# f_df6.to_excel(writer, 'Sheet1')
# writer.save()

##########################
# 7 入口流水表   k_df1 = 5 * 6 ，  k_df2 = 4 * 6

sql = "select * from entry_pos_flow"
df7 = pd.read_sql_query(sql, con=db)
a = df7.count()
print(a)

f_df7 = pd.merge(df7, k_df2, how='inner', on='merchant_ap')
a = f_df7.count()
print(a)

writer = pd.ExcelWriter('output7-1.xlsx')
f_df7.to_excel(writer, 'Sheet1')
writer.save()

# f_df7 = pd.merge(df7,k_df1,how='inner',on='merchant_ap')
# a = f_df7.count()
# print(a)

# writer = pd.ExcelWriter('output7-2.xlsx')
# f_df7.to_excel(writer, 'Sheet1')
# writer.save()
