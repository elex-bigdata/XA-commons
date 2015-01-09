#!/bin/bash

#working dir
day=`date -d "${date}" +%d`
year=`date -d "${date}" +%Y`
month=`date -d "${date}" +%m`

time=`date +%Y-%m-%d,%H:%M:%S`

workDir=/home/hadoop/xa
logDir=${workDir}/log
runJar=${workDir}/runJar

jar="XA-commons-1.0.0-jar-with-dependencies.jar";
testjar="xa-operation.jar";

pwd=$(cd "$(dirname "$0")"; pwd)
nowDir=`dirname $pwd`
dist=${nowDir}/target
deployBin=${nowDir}/bin/deployBin


#log deploy info
if [ ! -d ${logDir} ]
then
	mkdir -p ${logDir}
fi

chmod 777 -R ${logDir}
cd ${nowDir}
git pull


#ant
if [ ! -d $dist ]
then
	mkdir -p $dist
fi
cd ${nowDir}
mvn clean
mvn package

#***************
# test jar




#***************
#copy the jar
hostliststr="node0,node1,node2,node3,node4,node5,node6,node7,node8,node9,node10,node11,node12,node13,node14,node15"
host=`echo ${hostliststr}|awk '{split($1,a,",");for(key in a)print a[key];}'`
for node in ${host}
do
	echo ${node}
        echo ${dist}/${jar} ${node}${runJar}/${testjar}
	scp  ${dist}/${jar} ${node}:${runJar}/${testjar}
done




