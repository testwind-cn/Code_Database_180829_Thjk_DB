#!coding:utf-8

import os
from goodboss.overdueloan.wj_tools.tool_load_excel_notype import LoadExcel
from goodboss.overdueloan.wj_tools import sftpUtil
from goodboss.overdueloan.wj_tools.str_tool import StrTool

# https://xlrd.readthedocs.io/en/latest/api.html?highlight=Cell#xlrd.sheet.Cell

data_type_D0010 = [
    0, 2, 0, 1, 0, 1, 1, 1, 0, 1,
    1, 3, 3, 2, 2, 2, 2, 2, 1, 2,
    2, 2, 2, 2, 2, 2, 0, 2, 0, 2,
    0, 2, 2, 2, 2, 2, 2, 2, 0, 2,
    2, 2, 0, 2, 2, 2, 2, 2, 2, 2,
    2, 2, 2, 0, 1, 0, 2
    ]

s_sql = '''INSERT INTO d0010 ( 
col_01,col_02,col_03,col_04,col_05,col_06,col_07,col_08,col_09,col_10,
col_11,col_12,col_13,col_14,col_15,col_16,col_17,col_18,col_19,col_20,
col_21,col_22,col_23,col_24,col_25,col_26,col_27,col_28,col_29,col_30,
col_31,col_32,col_33,col_34,col_35,col_36,col_37,col_38,col_39,col_40,
col_41,col_42,col_43,col_44,col_45,col_46,data_dt
)
VALUES
( {} )
'''
s_sql2 = "DELETE from d0010 WHERE data_dt = str_to_date('{}','%Y%m%d')"


def load_excel_D0010(p_date, p_f_name='D0010LoanSurplusRpt_01.xls'):  # : str = u'D0010LoanSurplusRpt_01.xls'
    # u'D0010LoanSurplusRpt_01_修改_{}.xls'
    # file_name1 = os.path.join(os.getcwd(), u'D0010LoanSurplusRpt_01_修改_{}.xls'.format(p_date))

    file_name1 = os.path.join(os.getcwd(), 'data_tmp', p_date, p_f_name)

    a = LoadExcel()
    a.load_excel(p_file_name1=file_name1, p_title_row=4, p_total_col=57, p_sql=s_sql, p_sql2=s_sql2, p_data_type=data_type_D0010, the_date=p_date)


def download_excel_D0010(p_date, p_f_name='D0010LoanSurplusRpt_01.xls'):
    # /datateam/reports/REPORTS/20190325/S620000TranAdjSummary_000080000001_20190325.OK
    result = sftpUtil.getConnect("101.230.217.35", 9999, "report", "rep2018")
    file_name1 = "/datateam/reports/REPORTS/" + p_date + "/" + p_f_name
    file_name2 = os.path.join(os.getcwd(), 'data_tmp', p_date)
    if result[0] == 1:
        result2 = sftpUtil.download(result[2], file_name1, file_name2)
        sftpUtil.closeConnect(result[2])
        if result2[0] == 1:
            return True
        else:
            return False
    else:
        return False
        # myLog.Log('sftp 连接失败', False)


if __name__ == "__main__":
    date = "20190404"
    fname = 'D0010LoanSurplusRpt_01.xls'

    date = StrTool.get_the_date_str("", -1)
    print("================   " + date)
    if download_excel_D0010(date, fname):
        load_excel_D0010(date, fname)






