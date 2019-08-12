-- 正则替换掉注释      (?:^|[ ]+)(?:#|--) +[^\n]*

Hive06.mp4 ，Hive07.mp4  讲HIVE参数

/*
"$(cat <<EOF
xxx
EOF
)"
*/

-- xxx 里面，不能有 \t  不能有 `, 可以有'

/*
hive -e "$(cat <<EOF
sql
EOF)"
*/




-- hive --hivevar THE_DATE=20180502 -f main_sub_01.sql
-- SET hivevar:THE_DATE=20180502;

-- 默认 hive.exec.dynamic.partition=true
SET hive.exec.dynamic.partition=true;
-- 默认 hive.exec.dynamic.partition.mode=nonstrict
SET hive.exec.dynamic.partition.mode=nonstrict;
-- 默认 hive.exec.max.dynamic.partitions.pernode=100
SET hive.exec.max.dynamic.partitions.pernode = 4000;
-- 默认 hive.exec.max.dynamic.partitions=1000
SET hive.exec.max.dynamic.partitions=4000;


-- ===设置map输出和reduce输出进行合并的相关参数：
-- #设置map端输出进行合并，默认为true，hive.merge.mapfiles=true
-- set hive.merge.mapfiles = true;

-- #设置reduce端输出进行合并，默认为false，hive.merge.mapredfiles=false
set hive.merge.mapredfiles = true;

-- #设置合并文件的大小，默认 hive.merge.size.per.task=268435456
-- set hive.merge.size.per.task = 256000000;

-- #当输出文件的平均大小小于该值时，启动一个独立的MapReduce任务进行文件merge。
-- 默认 hive.merge.smallfiles.avgsize=16777216
-- set hive.merge.smallfiles.avgsize=16000000;

-- 默认 hive.enforce.bucketing=true
SET hive.enforce.bucketing = true;


-- 默认 hive.exec.compress.output=false
set hive.exec.compress.output=true;

-- 默认 mapred.output.compression.type=BLOCK
set mapred.output.compression.type=BLOCK;

-- 默认 mapred.output.compression.codec=org.apache.hadoop.io.compress.DefaultCodec
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;


sudo -u admin hive
hive>dfs -text /user/hive/warehouse/dw_2g.db/tmp_dwd_merchant_info/000000_0

hive>dfs -ls -h /user/hive/warehouse/dw_2g.db/tmp_dwd_merchant_info/ ;
    -rwxrwxrwt   3 admin hive     33.7 M 2019-07-26 15:25 /user/hive/warehouse/dw_2g.db/tmp_dwd_merchant_info/000000_0
    -rwxrwxrwt   3 admin hive     33.7 M 2019-07-26 15:25 /user/hive/warehouse/dw_2g.db/tmp_dwd_merchant_info/000001_0
    -rwxrwxrwt   3 admin hive     33.8 M 2019-07-26 15:25 /user/hive/warehouse/dw_2g.db/tmp_dwd_merchant_info/000002_0
    -rwxrwxrwt   3 admin hive     33.7 M 2019-07-26 15:25 /user/hive/warehouse/dw_2g.db/tmp_dwd_merchant_info/000003_0
    -rwxrwxrwt   3 admin hive     33.7 M 2019-07-26 15:25 /user/hive/warehouse/dw_2g.db/tmp_dwd_merchant_info/000004_0
    -rwxrwxrwt   3 admin hive     33.8 M 2019-07-26 15:25 /user/hive/warehouse/dw_2g.db/tmp_dwd_merchant_info/000005_0
    -rwxrwxrwt   3 admin hive     33.8 M 2019-07-26 15:25 /user/hive/warehouse/dw_2g.db/tmp_dwd_merchant_info/000006_0
    -rwxrwxrwt   3 admin hive     33.7 M 2019-07-26 15:24 /user/hive/warehouse/dw_2g.db/tmp_dwd_merchant_info/000007_0
    -rwxrwxrwt   3 admin hive     33.7 M 2019-07-26 15:25 /user/hive/warehouse/dw_2g.db/tmp_dwd_merchant_info/000008_0
    -rwxrwxrwt   3 admin hive     33.7 M 2019-07-26 15:25 /user/hive/warehouse/dw_2g.db/tmp_dwd_merchant_info/000009_0
    -rwxrwxrwt   3 admin hive     33.8 M 2019-07-26 15:25 /user/hive/warehouse/dw_2g.db/tmp_dwd_merchant_info/000010_0
    -rwxrwxrwt   3 admin hive     33.8 M 2019-07-26 15:25 /user/hive/warehouse/dw_2g.db/tmp_dwd_merchant_info/000011_0
    -rwxrwxrwt   3 admin hive     33.7 M 2019-07-26 15:25 /user/hive/warehouse/dw_2g.db/tmp_dwd_merchant_info/000012_0
    -rwxrwxrwt   3 admin hive     33.8 M 2019-07-26 15:25 /user/hive/warehouse/dw_2g.db/tmp_dwd_merchant_info/000013_0
    -rwxrwxrwt   3 admin hive     33.7 M 2019-07-26 15:25 /user/hive/warehouse/dw_2g.db/tmp_dwd_merchant_info/000014_0
    -rwxrwxrwt   3 admin hive     33.7 M 2019-07-26 15:25 /user/hive/warehouse/dw_2g.db/tmp_dwd_merchant_info/000015_0
    -rwxrwxrwt   3 admin hive     33.7 M 2019-07-26 15:24 /user/hive/warehouse/dw_2g.db/tmp_dwd_merchant_info/000016_0
    -rwxrwxrwt   3 admin hive     33.7 M 2019-07-26 15:24 /user/hive/warehouse/dw_2g.db/tmp_dwd_merchant_info/000017_0
    -rwxrwxrwt   3 admin hive     33.8 M 2019-07-26 15:24 /user/hive/warehouse/dw_2g.db/tmp_dwd_merchant_info/000018_0
    -rwxrwxrwt   3 admin hive     33.7 M 2019-07-26 15:24 /user/hive/warehouse/dw_2g.db/tmp_dwd_merchant_info/000019_0

默认压缩的 Sequence File
    /user/hive/warehouse/dw_2g.db/tmp_dwd_merchant_info/000000_0
        SEQ."hadoop.io.BytesWritable
        org.apache.hadoop.io.Text
        org.apache.hadoop.io.compress.DefaultCodec

hive>set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
指定压缩的 Sequence File
    /user/hive/warehouse/dw_2g.db/tmp_dwd_merchant_info/000000_0
        SEQ."org.apache.hadoop.io.BytesWritable
        org.apache.hadoop.io.Text
        org.apache.hadoop.io.compress.GzipCodec




[root@thfk-namenode01-1_100 appadm]# sudo -u admin hadoop fs -cat /user/hive/warehouse/dw_2g.db/tmp_dwd_merchant_info/000000_0 | more
DEPRECATED: Use of this script to execute hdfs command is deprecated.
Instead use the hdfs command for it.
SEQ"org.apache.hadoop.io.BytesWritableorg.apache.hadoop.io.Text'org.apache.hadoop.io.compress.GzipCodec
F󿿨 %
ҰΡ3b8ࠌ½𽭼
h-More--򁆞Ѱ C䡵ܼw➽sؕ^᧐


[root@thfk-namenode01-1_100 appadm]# sudo -u admin hadoop fs -text /user/hive/warehouse/dw_2g.db/tmp_dwd_merchant_info/000000_0 | more
DEPRECATED: Use of this script to execute hdfs command is deprecated.
Instead use the hdfs command for it.
19/07/26 16:21:18 INFO zlib.ZlibFactory: Successfully loaded & initialized native-zlib library
19/07/26 16:21:18 INFO compress.CodecPool: Got brand-new decompressor [.gz]
19/07/26 16:21:18 INFO compress.CodecPool: Got brand-new decompressor [.gz]
19/07/26 16:21:18 INFO compress.CodecPool: Got brand-new decompressor [.gz]
19/07/26 16:21:18 INFO compress.CodecPool: Got brand-new decompressor [.gz]
	UPDATE	821330159120054	99993300	浙江国药大药房有限公司	浙江国药大药房有限公司	48213300	330000000023528	5912	0	99993300	33	01    02	文二路225-8、225-9号	李胜	88805101			33000090	浙江国药大药房有限公司	0	2014-07-16		2	0	王怡文	      72008282-5	姚军	01_身份证	310101197210154419	0_日结	0	0	9_其它	01				李胜	15968123707	3301657 200082825	1	是	0	01	否	否	200033010050580	0	1500	药店连锁	药店连锁	2049-12-31	01	01	5912	      20180502	99990101

==============================================
默认 set hive.merge.mapredfiles = false;
得到12个小文件
==============================================
Total jobs = 1
Java HotSpot(TM) 64-Bit Server VM warning: ignoring option MaxPermSize=512M; support was removed in 8.0
Execution log at: /tmp/admin/admin_20190726142626_55643685-9841-49a7-bfdf-dbe862a81911.log
2019-07-26 02:27:00	Starting to launch local task to process map join;	maximum memory = 2025848832
2019-07-26 02:27:01	Dump the side-table for tag: 1 with group count: 3160 into file: file:/tmp/admin/d976e305-1b1e-442d-b705-814b445a896e/hive_2019-07-26_14-26-55_987_2612406016546535746-1/-local-10002/HashTable-Stage-4/MapJoin-mapfile01--.hashtable
2019-07-26 02:27:01	Uploaded 1 File to: file:/tmp/admin/d976e305-1b1e-442d-b705-814b445a896e/hive_2019-07-26_14-26-55_987_2612406016546535746-1/-local-10002/HashTable-Stage-4/MapJoin-mapfile01--.hashtable (105048 bytes)
2019-07-26 02:27:01	End of local task; Time Taken: 1.386 sec.
Execution completed successfully
MapredLocal task succeeded
Launching Job 1 out of 1
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1562031848110_20140, Tracking URL = http://nn1:8088/proxy/application_1562031848110_20140/
Kill Command = /opt/cloudera/parcels/CDH-5.13.0-1.cdh5.13.0.p0.29/lib/hadoop/bin/hadoop job  -kill job_1562031848110_20140
Hadoop job information for Stage-4: number of mappers: 12; number of reducers: 0
2019-07-26 14:27:10,600 Stage-4 map = 0%,  reduce = 0%
2019-07-26 14:27:24,270 Stage-4 map = 8%,  reduce = 0%, Cumulative CPU 12.69 sec
2019-07-26 14:27:26,379 Stage-4 map = 33%,  reduce = 0%, Cumulative CPU 57.74 sec
2019-07-26 14:27:28,487 Stage-4 map = 35%,  reduce = 0%, Cumulative CPU 87.46 sec
2019-07-26 14:27:29,522 Stage-4 map = 38%,  reduce = 0%, Cumulative CPU 184.85 sec
2019-07-26 14:27:34,708 Stage-4 map = 56%,  reduce = 0%, Cumulative CPU 213.26 sec
2019-07-26 14:27:35,745 Stage-4 map = 85%,  reduce = 0%, Cumulative CPU 251.12 sec
2019-07-26 14:27:39,869 Stage-4 map = 92%,  reduce = 0%, Cumulative CPU 259.96 sec
2019-07-26 14:27:41,935 Stage-4 map = 100%,  reduce = 0%, Cumulative CPU 274.59 sec
MapReduce Total cumulative CPU time: 4 minutes 34 seconds 590 msec
Ended Job = job_1562031848110_20140
Loading data to table dw_2g.dwd_merchant_info_history partition (act_end=20180502)
Partition dw_2g.dwd_merchant_info_history{act_end=20180502} stats: [numFiles=12, numRows=18960, totalSize=1889273, rawDataSize=0]
MapReduce Jobs Launched:
Stage-Stage-4: Map: 12   Cumulative CPU: 274.59 sec   HDFS Read: 3843786948 HDFS Write: 1890314 SUCCESS
Total MapReduce CPU Time Spent: 4 minutes 34 seconds 590 msec
OK
Time taken: 47.536 seconds





==============================================
set hive.merge.mapredfiles = true;
得到 1个合并文件
==============================================
Total jobs = 3
Java HotSpot(TM) 64-Bit Server VM warning: ignoring option MaxPermSize=512M; support was removed in 8.0
Execution log at: /tmp/admin/admin_20190726143030_79d0a0bd-c1be-45d4-9dac-d81934e739b7.log
2019-07-26 02:30:54	Starting to launch local task to process map join;	maximum memory = 2025848832
2019-07-26 02:30:57	Dump the side-table for tag: 1 with group count: 3160 into file: file:/tmp/admin/d976e305-1b1e-442d-b705-814b445a896e/hive_2019-07-26_14-30-50_416_1255912556544754835-1/-local-10003/HashTable-Stage-9/MapJoin-mapfile11--.hashtable
2019-07-26 02:30:57	Uploaded 1 File to: file:/tmp/admin/d976e305-1b1e-442d-b705-814b445a896e/hive_2019-07-26_14-30-50_416_1255912556544754835-1/-local-10003/HashTable-Stage-9/MapJoin-mapfile11--.hashtable (105048 bytes)
2019-07-26 02:30:57	End of local task; Time Taken: 2.322 sec.
Execution completed successfully
MapredLocal task succeeded
Launching Job 1 out of 3
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1562031848110_20143, Tracking URL = http://nn1:8088/proxy/application_1562031848110_20143/
Kill Command = /opt/cloudera/parcels/CDH-5.13.0-1.cdh5.13.0.p0.29/lib/hadoop/bin/hadoop job  -kill job_1562031848110_20143
Hadoop job information for Stage-9: number of mappers: 12; number of reducers: 0
2019-07-26 14:31:05,261 Stage-9 map = 0%,  reduce = 0%
2019-07-26 14:31:17,788 Stage-9 map = 17%,  reduce = 0%, Cumulative CPU 28.45 sec
2019-07-26 14:31:18,813 Stage-9 map = 33%,  reduce = 0%, Cumulative CPU 55.55 sec
2019-07-26 14:31:19,843 Stage-9 map = 58%,  reduce = 0%, Cumulative CPU 98.94 sec
2019-07-26 14:31:20,871 Stage-9 map = 75%,  reduce = 0%, Cumulative CPU 128.51 sec
2019-07-26 14:31:21,901 Stage-9 map = 94%,  reduce = 0%, Cumulative CPU 171.47 sec
2019-07-26 14:31:24,997 Stage-9 map = 97%,  reduce = 0%, Cumulative CPU 171.47 sec
2019-07-26 14:31:35,277 Stage-9 map = 100%,  reduce = 0%, Cumulative CPU 181.09 sec
MapReduce Total cumulative CPU time: 3 minutes 1 seconds 90 msec
Ended Job = job_1562031848110_20143
Stage-4 is filtered out by condition resolver.
Stage-3 is selected by condition resolver.
Stage-5 is filtered out by condition resolver.
Launching Job 3 out of 3
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1562031848110_20144, Tracking URL = http://nn1:8088/proxy/application_1562031848110_20144/
Kill Command = /opt/cloudera/parcels/CDH-5.13.0-1.cdh5.13.0.p0.29/lib/hadoop/bin/hadoop job  -kill job_1562031848110_20144
Hadoop job information for Stage-3: number of mappers: 1; number of reducers: 0
2019-07-26 14:31:43,229 Stage-3 map = 0%,  reduce = 0%
2019-07-26 14:31:48,434 Stage-3 map = 100%,  reduce = 0%, Cumulative CPU 2.83 sec
MapReduce Total cumulative CPU time: 2 seconds 830 msec
Ended Job = job_1562031848110_20144
Loading data to table dw_2g.dwd_merchant_info_history partition (act_end=20180502)
Partition dw_2g.dwd_merchant_info_history{act_end=20180502} stats: [numFiles=1, numRows=22120, totalSize=1889273, rawDataSize=0]
MapReduce Jobs Launched:
Stage-Stage-9: Map: 12   Cumulative CPU: 181.09 sec   HDFS Read: 3843785556 HDFS Write: 1890314 SUCCESS
Stage-Stage-3: Map: 1   Cumulative CPU: 2.83 sec   HDFS Read: 1899419 HDFS Write: 1889273 SUCCESS
Total MapReduce CPU Time Spent: 3 minutes 3 seconds 920 msec
OK
Time taken: 59.467 seconds
