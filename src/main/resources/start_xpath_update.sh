#!/bin/bash
source /etc/profile


# 启动python脚本的shell
# xpath配置文件，2天前的要删掉

delete_day=`date -d -2day '+%Y%m%d'`

echo delete_day:$delete_day

# 这段代码跑完后，直接生成配置文件，然后直接上传到 hdfs
/usr/local/bin/python /home/qingniu/hainiu_cralwer/download_page/xpath_config.py

rm -rf /home/qingniu/xpath_cache_file/xpath_file${delete_day}*
hadoop fs -rmr hdfs://ns1/user/qingniu/xpath_cache_file/xpath_file${delete_day}*
