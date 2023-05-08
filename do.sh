#!/bin/bash
# 本脚本运行在 mac 或者 linux key 自动把 jar 一键部署到 yarn
jar=flink_tuning-1.0-SNAPSHOT.jar
localDir=/Users/lzc/Desktop/class_code/flink_tuning/target
remoteHost=hadoop162
remoteUser=atguigu
remoteDir=/home/atguigu/flink_tuning

# copy本地 jar 发送到远程设备
rsync -rvl ${localDir}/${jar} ${remoteUser}@${remoteHost}:${remoteDir}/
#----------------------------------------------------------------------------
flink=/opt/module/flink-1.13.6/bin/flink
remoteJar=${remoteDir}/${jar}
class=com.atguigu.bigdata.tune.TMSlotApp
otherArgs=" \
-p 2 \
"
cmd="${flink} run -d \
     -t yarn-per-job \
     ${otherArgs} \
     -c ${class} \
     ${remoteJar} "
echo "${cmd}"
ssh ${remoteUser}@${remoteHost} "${cmd}"
