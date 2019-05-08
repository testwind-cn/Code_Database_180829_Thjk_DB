#!coding:utf-8

import os
from py_tools.file_load_base import FileLoad
from py_tools import sftpUtil
from py_tools.str_tool import StrTool
from py_tools.tool_mysql import TheDB
import pathlib

# https://xlrd.readthedocs.io/en/latest/api.html?highlight=Cell#xlrd.sheet.Cell

# 0 没有，2 整数，1 字符串，3 日期

data_pos_s6700 = [
    0,
    20,  # 000080000001
    25,  # line
    100,  # 1002
    130,  # ,按月还POS贷
    161,  # 990193058126027
    262,  # 城阳区董正扬调味品店
    343,  # 江海燕
    364,  # 68950000000000189932
    369,  # 双周
    380,  # 2018-04-26
    391,  # 2019-01-26
    402,  # 275
    418,  # 150000.00
    431,  # 360
    461,  # 等额本息
    477,  # 514.48
    493,  # 84.78
    509,  # 0.00
    525,  # 0.00
    541,  # 382.66
    558,  # 399233.90
    570,  # 0
    600,  # 活动状态(active)
    613,  # 0.2100
    626,  # 0.2520
    639,  # 0.2940
    652,  # 2
    665,  # 0
    678,  # 0
]
# 0 没有，2 整数，1 字符串，3 日期
data_type_s6700 = [
    1, 1, 1, 1, 1, 1, 1, 1, 1, 3,
    3, 2, 2, 2, 1, 2, 2, 2, 2, 2,
    2, 2, 1, 2, 2, 2, 2, 2, 2
    ]

s_sql = """INSERT INTO s6700 ( 
col_01,col_02,col_03,col_04,col_05,col_06,col_07,col_08,col_09,col_10,
col_11,col_12,col_13,col_14,col_15,col_16,col_17,col_18,col_19,col_20,
col_21,col_22,col_23,col_24,col_25,col_26,col_27,col_28,col_29,data_dt
)
VALUES
{} ;"""

s_sql2 = "DELETE from s6700 WHERE data_dt = str_to_date('{}','%Y-%m-%d')"


"""
CREATE TABLE `s6700` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `col_01` varchar(20) DEFAULT NULL COMMENT '机构号ORG_NO',
  `col_02` varchar(20) DEFAULT NULL COMMENT 'SERIAL',
    `col_03` varchar(20) DEFAULT NULL COMMENT '产品编号PRODUCT',
    `col_04` varchar(20) DEFAULT NULL COMMENT '产品描述PRD_DESC',
    `col_05` varchar(20) DEFAULT NULL COMMENT '商户号MCHT_CD',
    `col_06` varchar(40) DEFAULT NULL COMMENT '商户名称MCHT_NAME',
    `col_07` varchar(20) DEFAULT NULL COMMENT '法人姓名STLM_NM',
    `col_08` varchar(40) DEFAULT NULL COMMENT '贷款编号DB_NO',
    `col_09` varchar(20) DEFAULT NULL COMMENT '周期类型PERIOD_TYPE',
    `col_10` date DEFAULT NULL COMMENT '借款日期LOAN_DATE',
    `col_11` date DEFAULT NULL COMMENT '到期日期LOAN_MATURITY_DATE',
    `col_12` int(11) DEFAULT NULL COMMENT '贷款时长（天）PERIOD',
    `col_13` decimal(15,4) DEFAULT NULL COMMENT '贷款本金LOAN_PRINCIPAL',
    `col_14` int(11) DEFAULT NULL COMMENT '贷款总期数LOAN_PERIOD_NUM',
    `col_15` varchar(20) DEFAULT NULL COMMENT '还款方式REPAYMENT_METHOD',
    `col_16` decimal(15,4) DEFAULT NULL COMMENT 'SP_PRINCIPAL',
    `col_17` decimal(15,4) DEFAULT NULL COMMENT 'SP_FEE',
    `col_18` decimal(15,4) DEFAULT NULL COMMENT 'SP_FEE1',
    `col_19` decimal(15,4) DEFAULT NULL COMMENT 'SP_FEE2',
    `col_20` decimal(15,4) DEFAULT NULL COMMENT 'MIN_PAY',
    `col_21` decimal(15,4) DEFAULT NULL COMMENT 'LOAN_BALANCE',
    `col_22` int(11) DEFAULT NULL COMMENT 'OVERDUE_PERIOD',
    `col_23` varchar(20) DEFAULT NULL COMMENT 'STATUS',
    `col_24` decimal(9,6) DEFAULT NULL COMMENT 'RATE',
    `col_25` decimal(9,6) DEFAULT NULL COMMENT 'RATE1',
    `col_26` decimal(9,6) DEFAULT NULL COMMENT 'RATE2',
    `col_27` int(11) DEFAULT NULL COMMENT 'CURR_PERIOD',
    `col_28` int(11) DEFAULT NULL COMMENT 'OVERDUE_DAYS',
    `col_29` int(11) DEFAULT NULL COMMENT 'OVERDUE_2',
  `data_dt` date DEFAULT NULL COMMENT '报表日期',
  PRIMARY KEY (`id`),
  INDEX index_data_dt (`data_dt`)
) ENGINE=InnoDB AUTO_INCREMENT=13463 DEFAULT CHARSET=utf8mb4;
"""


"""
0,
20 000080000001
25 line
100 1002
130,按月还POS贷
161 990193058126027
262 城阳区董正扬调味品店
343 江海燕
364 68950000000000189932
369 双周
380 2018-04-26
391 2019-01-26
402 275
418 150000.00
431 360
461 等额本息
477 514.48
493 84.78
509 0.00
525 0.00
541 382.66
558 399233.90
570 0
600 活动状态(active)
613 0.2100
626 0.2520
639 0.2940
652 2
665  0
678 0
"""


def load_text_s6700(p_the_db: TheDB, p_table, p_date, p_f_name='S6700LoanRecord_000080000001_{}'):  # : str =
    date_08 = StrTool.get_the_date_str(p_date, 0, 8)
    file_name1 = os.path.join(os.getcwd(), '..', '..', '..', 'data_tmp', date_08, p_f_name.format(date_08))
    path_1 = pathlib.Path(file_name1)
    file_name1 = str(path_1.resolve(strict=False))

    a = FileLoad(p_file_name1=file_name1, p_first_row=1, p_total_col=29, p_ctl_col=-1, p_data_type=data_type_s6700,p_data_pos=data_pos_s6700)
    a.LoadHive_Mysql(p_thedate=p_date, file_type=1, p_the_db=p_the_db, p_table=p_table, p_sql=s_sql, p_sql2=s_sql2)


def download_text_s6700(p_date, p_f_name='S6700LoanRecord_000080000001_{}'):
    # /datateam/reports/REPORTS/20190325/S620000TranAdjSummary_000080000001_20190325.OK
    date_08 = StrTool.get_the_date_str(p_date, 0, 8)
    result = sftpUtil.getConnect("172.31.130.14", 22, "report", "rep2018")
    file_name1 = "/datateam/reports/REPORTS/" + date_08 + "/" + p_f_name.format(date_08)
    file_name2 = os.path.join(os.getcwd(), '..', '..', '..', 'data_tmp', date_08)
    path_1 = pathlib.Path(file_name2)
    file_name2 = str(path_1.resolve(strict=False))

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
    """
    date = "20190329"
    fname = 'D0009LoanSurplusRpt_1.xls'

    for i in range(0, 342):
        date = StrTool.get_the_date_str("20180427", i)
        print("================   " + date)
        if download_excel_D0009(date, fname):
            load_excel_D0009(date, fname)
"""

    date = "201804027"
    fname = 'S6700LoanRecord_000080000001_{}'

    the_db = TheDB()
    the_db.connect(
        host='10.91.1.19',
        port=3306,
        user='risk',
        passwd='fTO@J5jmW&Q4',
        db='thbl_rpt'
    )

    # for i in range(0, 357):
    #     date_10 = StrTool.get_the_date_str("20180427", i, 10)
    #     print("================   " + date_10)
    #     if download_text_s6700(date_10, fname):
    #         load_text_s6700(p_the_db=None, p_table="allinpal_rpt.thbl_rpt_s6700", p_date=date_10, p_f_name=fname)

    date_10 = StrTool.get_the_date_str("", -1, 10)
    print("================   " + date_10)
    if download_text_s6700(date_10, fname):
        load_text_s6700(p_the_db=the_db, p_table="allinpal_rpt.thbl_rpt_s6700", p_date=date_10, p_f_name=fname)

    # 关闭连接
    the_db.close()


