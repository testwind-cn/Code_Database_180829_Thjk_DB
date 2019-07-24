#!/bin/bash
# 在100上运行

thedir="$(dirname $0)/"

cd ${thedir}

${thedir}drop.sh dw_2g.dim_code_manager_tmp

sudo -u admin hive -f ${thedir}main_sub_01.sql

${thedir}drop.sh dw_2g.dim_code_manager

sudo -u admin hive -f ${thedir}main_sub_02.sql