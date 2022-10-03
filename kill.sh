#!/bin/bash
remoteHost=hadoop162
remoteUser=atguigu
ssh ${remoteUser}@${remoteHost} \
"yarn application -list 1>/dev/null 2>&1 \
 | awk '/application_/{print \$1}' \
 | xargs yarn application -kill 1>/dev/null 2>&1
"

