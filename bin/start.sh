#!/bin/bash

#working dir

time=`date +%Y-%m-%d,%H:%M:%S`

workDir=/home/hadoop/xa
logDir=${workDir}/log
runJar=${workDir}/runJar

jar="xa-operation.jar";

#hadoopsh="/usr/lib/hadoop/bin/hadoop"

fileencoding="-Dfile.encoding=UTF-8"
verboses="-XX:+HeapDumpOnOutOfMemoryError"
memarg="-server -Xms4g -Xmx4g -Xss256K"
gcarg="-XX:SurvivorRatio=16 -XX:+UseConcMarkSweepGC -XX:NewSize=512M -XX:MaxNewSize=512M -XX:+UseAdaptiveSizePolicy -XX:-ExplicitGCInvokesConcurrent -XX:+UseCMSCompactAtFullCollection -XX:CMSFullGCsBeforeCompaction=2"

main="com.xingcloud.operations.MysqlOperation"

hostliststr="node0,node1,node2,node3,node4,node5,node6,node7,node8,node9,node10,node11,node12,node13,node14,node15"
host=`echo ${hostliststr}|awk '{split($1,a,",");for(key in a)print a[key];}'`
for node in ${host} 
do
	echo ${node}
	echo "beforekill"
	ssh ${node} ps aux|grep $main|awk '{print$2}'
	pidlist=`ssh ${node} ps aux|grep $main|awk '{print$2}'`
for pid in $pidlist
do
echo $pid
ssh ${node} kill $pid
done
    echo "afterkill"
    ssh ${node} ps aux|grep $main|awk '{print$2}'
    ssh ${node} nohup /usr/java/jdk/bin/java $fileencoding $memarg $gcarg $verboses -classpath ${runJar}/${jar} $main > /dev/null 2>&1 &
    #ssh ${node} nohup ${hadoopsh} jar ${runJar}/${jar} $main   >/dev/null 2>&1 &
done
