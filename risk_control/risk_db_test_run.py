import pymysql.cursors


# 连接数据库
connect = pymysql.Connect(
    host='172.31.64.115',
    port=3306,
    user='pub_query',
    passwd='Pub_query123',
    db='loan',
    charset='utf8'
)

# 获取游标
cursor = connect.cursor()

# 事务处理
    # 插入数据

sql = "select * from  loan.t_loan_credit_req where  CAST(IFNULL(loan.t_loan_credit_req.RECORD_NO,-1) as DECIMAL(31))  in ( '14989851318730650729896', '15030611405000650720752', '15120287707240650720076','15233301792280700245782','15248207867480700245240','15268047381700700249116' ) "
# sql = "INSERT INTO user_credit_report_history_test_wj (report_location, report_id, report_req_date,report_gen_date,report_user_name,report_user_id_no) VALUES ( '%s', '%s', str_to_date('%s','%%Y.%%m.%%d %%T'),str_to_date('%s','%%Y.%%m.%%d %%T'), '%s', '%s' )"

cmdSQL = sql
# cmdSQL = sql % data

#    CAST(IFNULL(loan.t_loan_credit_req.RECORD_NO,-1) as DECIMAL(31)) not in ( '14989851318730650729896', '15030611405000650720752',
#  '15120287707240650720076','15233301792280700245782','15248207867480700245240','15268047381700700249116' )

cursor.execute('SET NAMES UTF8MB4')

try:
    cursor.execute(cmdSQL)  # 添加数据
except Exception as e:
    connect.rollback()  # 事务回滚
    print('事务处理失败', e)
#    a = cursor.
else:
    connect.commit()  # 事务提交
    print('事务处理成功', cursor.rowcount)


# 关闭连接
cursor.close()
connect.close()
