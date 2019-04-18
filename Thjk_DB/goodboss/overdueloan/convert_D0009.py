#!coding:utf-8

import os
import pandas as pd
import numpy as np
from pandas import Series,DataFrame
import xlrd
import xlwt
import datetime

# ctype : 0 empty,1 string, 2 number, 3 date, 4 boolean, 5 error
# https://github.com/python-excel/xlwt/blob/master/examples/num_formats.py
data_type_D0009 = [
    0, 2, 0, 0, 1, 0, 1, 0, 1, 1,
    0, 0, 0, 1, 1, 3, 3, 2, 2, 2,
    0, 0, 2, 2, 0, 1, 0, 2, 2, 2,
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
    2, 2, 2, 0, 2, 0, 2, 0, 2, 2,
    2, 2, 2, 2, 2, 2, 2, 2, 2, 1,
    1, 2
    ]

data_type_D0010 = [
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 3, 3, 2, 2, 2, 2, 2, 1, 2,   # 18'日‘， 19,50000
    2, 2, 2, 2, 2, 2, 1, 2, 1, 2,
    1, 2, 2, 2, 2, 2, 2, 2, 1, 2,  # 37='59,221.99'  38='' , 39=500000
    2, 2, 1, 2, 2, 2, 2, 2, 2, 2,  # 47='59,221.99', 48=0, 49=0
    2, 2, 2, 1, 1, 1, 2
    ]


def convert_excel(p_file_name1: str, p_file_name2: str, p_title_row: int, p_total_col: int, p_data_type: list):

    workbook1 = xlrd.open_workbook(p_file_name1)
    workbook2 = xlwt.Workbook()

    sheet_names = workbook1.sheet_names()

    for sheet_name in sheet_names:
        sheet1 = workbook1.sheet_by_name(sheet_name)
        print(sheet_name)

        sheet2 = workbook2.add_sheet(sheet_name)

        rows = sheet1.row_values(1)  # 获取第2行内容
        print(rows)

        rows = sheet1.row_values(3)  # 获取第四行内容
        print(rows)

        rows = sheet1.row_values(4)  # 获取第5行内容
        print(rows)

        # 把标题栏写入到第一行
        for col in range(0, p_total_col):
            cell_value = sheet1.cell_value(p_title_row, col)  # 获取第i+行内容
            sheet2.write(0, col, cell_value)  # 第0行第i列写入内容
            print(cell_value)

        row_n = 1
        row_id = "DATA"

        style1 = xlwt.XFStyle()
        style1.num_format_str = 'yyyy-mm-dd'

        while len(row_id.strip()) > 0 and row_id.strip() != u'合计':

            row_id = "DATA"
            # 下面2行是监控
            rows_value = sheet1.row_values(row_n + p_title_row)
            print(rows_value)

            for col in range(0, p_total_col):
                cell_value = sheet1.cell_value( row_n + p_title_row, col)

                t = sheet1.cell_type(row_n+p_title_row, col)

                # 第2列是合计
                if col == 1:
                    row_id = cell_value
                    if len(row_id.strip()) <= 0 or row_id.strip() == u'合计':
                        break

                if p_data_type[col] == 3:
                    sdate1 = datetime.datetime.strptime(cell_value, "%Y-%m-%d").date()
                    sheet2.write(row_n, col, sdate1, style1)
                elif p_data_type[col] == 2:
                    cell_value = cell_value.replace(',', '')
                    cell_value = float(cell_value)
                    sheet2.write(row_n, col, cell_value)
                else:
                    sheet2.write(row_n, col, cell_value)

            row_n += 1

        workbook2.save(p_file_name2)


def convert_excel_D0009(p_file_name1: str = u'D0009LoanSurplusRpt_1.xls', p_file_name2: str = u'D0009LoanSurplusRpt_1_修改.xls'):
    return convert_excel(p_file_name1=p_file_name1, p_file_name2=p_file_name2, p_title_row=3, p_total_col=62, p_data_type=data_type_D0009)


def convert_excel_D0010(p_file_name1: str = u'D0010LoanSurplusRpt_01.xls', p_file_name2: str = u'D0010LoanSurplusRpt_01_修改.xls'):
        return convert_excel(p_file_name1=p_file_name1, p_file_name2=p_file_name2, p_title_row=4, p_total_col=57, p_data_type=data_type_D0010)


def cal_data1(p_data, overduedays, startdate, enddate, status=''):   # status='逾期终止'
    aaa = p_data[p_data['逾期天数'] >= overduedays]   # 30 / 90
    if len(status) > 0:
        ddd = aaa[aaa['贷款状态'] == status]
    else:
        ddd = aaa
        status = '合计'
    bbb = (ddd.set_index('贷款起息日'))[startdate:enddate]
    a1 = bbb['序号'].count()
    a2 = round(bbb['贷款本金'].sum(), 2)
    a3 = round(bbb['本金.2'].sum(), 2)

    print("{}\t{}\t{:<10s}\t{}\t{}\t{}".format(startdate, enddate, status, a1, a2, a3))
    return a1, a2, a3


def cal_data2(p_file, days):
    f_file1 = p_file
    f_data = pd.read_excel(f_file1)

    print("----------------{}  days  --BEGIN---\n".format(days))

    r1, r2, r3 = cal_data1(f_data, days, '2015-1-1', '2015-12-31', '逾期终止')
    r1, r2, r3 = cal_data1(f_data, days, '2015-1-1', '2015-12-31', '到期未结清')
    r1, r2, r3 = cal_data1(f_data, days, '2015-1-1', '2015-12-31', '活动中')
    r1, r2, r3 = cal_data1(f_data, days, '2015-1-1', '2015-12-31')
    print("----------^^" + '2015-1-1' + '2015-12-31')

    r1, r2, r3 = cal_data1(f_data, days, '2016-1-1', '2016-12-31', '逾期终止')
    r1, r2, r3 = cal_data1(f_data, days, '2016-1-1', '2016-12-31', '到期未结清')
    r1, r2, r3 = cal_data1(f_data, days, '2016-1-1', '2016-12-31', '活动中')
    r1, r2, r3 = cal_data1(f_data, days, '2016-1-1', '2016-12-31')
    print("----------^^"+'2016-1-1'+'2016-12-31')

    r1, r2, r3 = cal_data1(f_data, days, '2017-1-1', '2017-12-31', '逾期终止')
    r1, r2, r3 = cal_data1(f_data, days, '2017-1-1', '2017-12-31', '到期未结清')
    r1, r2, r3 = cal_data1(f_data, days, '2017-1-1', '2017-12-31', '活动中')
    r1, r2, r3 = cal_data1(f_data, days, '2017-1-1', '2017-12-31')
    print("----------^^"+'2017-1-1'+'2017-12-31')

    r1, r2, r3 = cal_data1(f_data, days, '2018-1-1', '2018-12-31', '逾期终止')
    r1, r2, r3 = cal_data1(f_data, days, '2018-1-1', '2018-12-31', '到期未结清')
    r1, r2, r3 = cal_data1(f_data, days, '2018-1-1', '2018-12-31', '活动中')
    r1, r2, r3 = cal_data1(f_data, days, '2018-1-1', '2018-12-31')
    print("----------^^"+'2018-1-1'+'2018-12-31')

    r1, r2, r3 = cal_data1(f_data, days, '2019-1-1', '2019-12-31', '逾期终止')
    r1, r2, r3 = cal_data1(f_data, days, '2019-1-1', '2019-12-31', '到期未结清')
    r1, r2, r3 = cal_data1(f_data, days, '2019-1-1', '2019-12-31', '活动中')
    r1, r2, r3 = cal_data1(f_data, days, '2019-1-1', '2019-12-31')
    print("----------^^" + '2019-1-1' + '2019-12-31')

    r1, r2, r3 = cal_data1(f_data, days, '2015-1-1', '2019-12-31', '逾期终止')
    r1, r2, r3 = cal_data1(f_data, days, '2015-1-1', '2019-12-31', '到期未结清')
    r1, r2, r3 = cal_data1(f_data, days, '2015-1-1', '2019-12-31', '活动中')
    r1, r2, r3 = cal_data1(f_data, days, '2015-1-1', '2019-12-31')
    print("----------^^"+'2015-1-1'+'2019-12-31')

    print("----------------{}  days  --END---\n".format(days))


def cal_data3(p_file, p_file_out):
    """
    用Pandas 计算放款变化趋势
    :param p_file:
    :param p_file_out:
    :return:
    """
    f_file1 = p_file
    f_data = pd.read_excel(f_file1)

    bbb = f_data.query('逾期天数>=90', inplace=False)  # inplace参数表示是否在原数据上操作inplace=False将会生成新的DataFrame
    print(bbb['序号'].count())
    aaa = f_data[f_data['逾期天数'] >= 90]   # 30 / 90
    print(aaa['序号'].count())

    ccc = f_data.eval("""
    NEW1 = 贷款本金 * 贷款期限
    NEW2 =贷款起息日""", inplace=False)
    # datetime.datetime.
    ccc['NO1'] = ccc.apply(lambda x: (int(x['序号']) if (str(x['序号']).isdigit()) else 0), axis=1)
    ccc = ccc.query('NO1>0', inplace=False)

    ccc['本金*天数'] = ccc.apply(lambda x: x['贷款本金'] * x['贷款期限'], axis=1)
    ccc['年月组'] = ccc.apply(lambda x: pd.to_datetime(x['贷款起息日']).strftime("%Y%m"), axis=1)

    print(ccc['序号'].count())

    ddd1 = ccc.groupby('年月组').agg(
        {
            '贷款本金': {'每月贷款本金': (lambda x: np.sum(abs(x))) },
            '本金*天数': {'每月本金*天数': np.sum}
        }
    )

    # ddd2 = ccc.groupby('年月组').agg(
    #     {
    #         '贷款本金': {'每月放款额': np.sum},
    #         '资金额度X时长': {'每月资金额度时长': np.sum}
    #     }
    # )

    ddd2 = ccc.groupby('年月组').agg({'贷款本金': np.sum,'本金*天数': np.sum})

    ddd2['平均借款天数'] = ddd2.apply(lambda x: x['本金*天数'] / x['贷款本金'], axis=1)

    ddd2.to_excel(p_file_out)

    print(ddd2.count())
    # >>> df.groupby('A').agg({'B': ['min', 'max'], 'C': 'sum'})
    # 按A分组，对B做min和max，对C做sum


if __name__ == "__main__":
    date = "20190403"

    print(os.getcwd())
    file_name1 = os.path.join(os.getcwd(), 'data_tmp', date, 'D0009LoanSurplusRpt_1.xls')
    file_name2 = os.path.join(os.getcwd(), 'data_tmp', u'D0009LoanSurplusRpt_1_修改_{}.xls'.format(date))
    print(file_name1)

    convert_excel_D0009(p_file_name1=file_name1, p_file_name2=file_name2)

    cal_data2(file_name2, 30)
    cal_data2(file_name2, 90)

    # #####################################################
    # 用Pandas 计算放款变化趋势
    # 用D0009计算是不对的，下面的是用D0010来计算
    # cal_data3(file_name2,"output_D0009.xlsx")

    file_name1 = os.path.join(os.getcwd(), date, 'D0010LoanSurplusRpt_01.xls')
    file_name2 = os.path.join(os.getcwd(), u'D0010LoanSurplusRpt_01_修改_{}.xls'.format(date))
    print(file_name1)

    # convert_excel_D0010(p_file_name1=file_name1, p_file_name2=file_name2)

    # cal_data3(file_name2, "output_D0010.xlsx")






