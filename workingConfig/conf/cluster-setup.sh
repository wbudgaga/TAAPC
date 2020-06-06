#!/bin/bash
#	read slaves files from conf directory
#	and create hadoop local directories in /tmp
if [ $# -eq 1 ] ; then
	conf_dir=$1
else
	conf_dir=$HOME/hadoop/conf
fi

slaves=$(cat $conf_dir/slaves)
user='budgaga'
echo $user
echo  ${slaves[*]}
for host in ${slaves[*]}
do
#        ssh $host chmod -R 777 /tmp/hadoop-budgaga/*
#	ssh $host rm -Rf /tmp/hadoop-${user}/*
	ssh $host mkdir -p /tmp/hadoop-${user}/dfs/data /tmp/hadoop-${user}/logs
	ssh $host chmod -R 755 /tmp/hadoop-${user}/dfs/data /tmp/hadoop-${user}/logs
done
