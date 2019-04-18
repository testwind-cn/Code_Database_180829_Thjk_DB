#!/usr/bin/ python
# -*- coding: utf-8 -*-

"""
使用CVS包，测试读取20190122收到的通金的商户统计数据表文件
"""

import csv


from_file = "D:\\下载\\posflow\\20190122_mchtInfo\\TM_BRANCH_INFO.prd.all.del\\TM_BRANCH_INFO.prd.all.del"
f1 = open(from_file, 'r',  newline='', encoding="gb18030")

f_reader = csv.reader(f1, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL,
                      doublequote=False, escapechar='\\')

i = 0
for row in f_reader:
    print(str(i) + "   " +  str(len(row))+'\n')
    if len(row) <= 0:
        continue
    last = row
    i = i + 1
