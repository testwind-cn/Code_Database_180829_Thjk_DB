#!coding:utf-8
import pathlib
import os
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
    fname: str = __file__
    pos = str(fname).rfind('.')
    if pos > 0:
        fname = fname[0:pos]

    conn = connect(host="10.91.1.100", port=conf.hive_port(), auth_mechanism=conf.hive_auth(), user=conf.hive_user())
    cur = conn.cursor()

    if os.path.isfile(fname+".ini"):
        f_file = open(fname+".ini", 'r', encoding="utf-8")  # 返回一个文件对象
        line = f_file.readline()
        while len(line) > 0:
            if len(line) > 5:
                print("Run: " + "  " + line + "\n")
                cur.execute(line)  # , async=True)
                print("OK: " + "  " + line + "\n")
            else:
                print("Pass: " + "  " + line + "\n")
            line = f_file.readline()
        f_file.close()

    sql = ""
    if os.path.isfile(fname + ".sql"):
        f_file = open(fname + ".sql", 'r', encoding="utf-8")  # 返回一个文件对象
        line = f_file.readline()
        while len(line) > 0:
            sql += line
            line = f_file.readline()
        f_file.close()

    if len(sql) > 10:
        print("Run: " + "  " + sql + "\n")
        cur.execute(sql)  # , async=True)
        print("OK: SQL" + "\n")
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


