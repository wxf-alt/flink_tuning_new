#!/bin/bash
# 本脚本一键杀死yarn 上所有的app
remoteHost=hadoop162
remoteUser=atguigu
ssh ${remoteUser}@${remoteHost} \
"yarn application -list 1>/dev/null 2>&1 \
 | awk '/application_/{print \$1}' \
 | xargs yarn application -kill 1>/dev/null 2>&1
"

