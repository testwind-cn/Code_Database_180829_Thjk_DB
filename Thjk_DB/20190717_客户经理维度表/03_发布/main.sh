#!/bin/bash
# 在100上运行

thedir="$(dirname $0)/"

cd ${thedir}

${thedir}drop.sh dim.tmp_dim_manager_code

sudo -u admin hive -f ${thedir}main_sub_01.sql

${thedir}drop.sh dim.dim_manager_code

sudo -u admin hive -f ${thedir}main_sub_02.sql