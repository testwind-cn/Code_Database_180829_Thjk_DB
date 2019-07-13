#!coding:utf-8

import pandas as pd
from impala.dbapi import connect
from impala.util import as_pandas
from hdfs.client import Client
from impala.dbapi import connect
from wj_tools import sftp_tool
from wj_tools.file_check import MyLocalFile
from wj_tools.file_check import MyHdfsFile
from wj_tools.hdfsclient import MyClient  # hdfs
# hive
# data path config file
from conf import ConfigData
from wj_tools.str_tool import StrTool


def run_hive(conf: ConfigData, the_date: str):
    conn = connect(host="10.91.1.100", port=conf.hive_port(), auth_mechanism=conf.hive_auth(), user=conf.hive_user())
    cur = conn.cursor()

    sql = """
    --qsdai sql created by WangJun on 11-Jul-2019 am 10:31
use ods_ftp
"""
    print("OK" + "  " + sql + "\n")
    cur.execute(sql)  # , async=True)

    sql = "set mapreduce.job.queuename = qsdai;"
    print("OK" + "  " + sql + "\n")
    cur.execute(sql)  # , async=True)
    sql = "set hive.cli.print.header=true;"
    print("OK" + "  " + sql + "\n")
    cur.execute(sql)  # , async=True)

    sql2 = """
    SELECT * from ods_ftp.qsd_merchant    
"""
    print("OK" + "  " + sql2+"\n")
    cur.execute(sql2)  # , async=True)
    data = as_pandas(cur)

    print(len(data))

    name = '/home/data/qsdai/qsdai_'+StrTool.get_the_date_str('')+'.xlsx'

    writer = pd.ExcelWriter(name)

    data.to_excel(writer, 'Sheet1')

    writer.save()

    cur.close()
    conn.close()


if __name__ == "__main__":
    the_conf = ConfigData(p_is_test=False)

    run_hive(the_conf, the_date="")


