#!/bin/bash
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
# class=com.atguigu.bigdata.tune.TMSlotApp
#class=com.atguigu.bigdata.tune.RocksdbTuningApp
#class=com.atguigu.bigdata.tune.NoUUidApp
#class=com.atguigu.bigdata.tune.UUidApp
#class=com.atguigu.bigdata.tune.BackpressureApp
#class=com.atguigu.bigdata.tune.SkewApp1
#class=com.atguigu.bigdata.tune.SkewApp2
class=com.atguigu.bigdata.tune.SqlApp
otherArgs=" \
-p 4 \
-Drest.flamegraph.enabled=true \
-Dtaskmanager.numberOfTaskSlots=4 \
-Dtaskmanager.memory.process.size=4096m \
-Dstate.backend.latency-track.keyed-state-enabled=true \
-Dmetrics.latency.interval=30000 \
"
main_args=" --demo distinct --minibatch true --split-distinct true "
# 通过 ssh 的方式在远程提交 jar

cmd="${flink} run -d \
     -t yarn-per-job \
     ${otherArgs} \
     -c ${class} \
     ${remoteJar} \
     ${main_args} "
echo "${cmd}"
ssh ${remoteUser}@${remoteHost} "${cmd}"
