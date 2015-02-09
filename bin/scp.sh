#!/bin/bash

src="/home/hadoop/wanghaixing/mysqldump.py"
des="~/"

hostliststr="node0,node1,node2,node3,node4,node5,node6,node7,node8,node9,node10,node11,node12,node13,node14,node15"
host=`echo ${hostliststr}|awk '{split($1,a,",");for(key in a)print a[key];}'`
for node in ${host}
do
    echo ${node}
    `scp ${src} ${node}:${des}`
done