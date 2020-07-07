#!/bin/bash

if [ "$1" != "" ]; then

     /usr/local/hadoop-0.23.1/bin/hdfs dfs -rm -r  hadoop/bigrams*
     /usr/local/hadoop-0.23.1/bin/hadoop jar ./cs455.jar cs455.BigramCreator hadoop/books hadoop/bigrams

fi

/usr/local/hadoop-0.23.1/bin/hdfs dfs -rm -r  hadoop/remove


/usr/local/hadoop-0.23.1/bin/hadoop jar ./cs455.jar cs455.RemoveRedundantWord hadoop/probabilities hadoop/remove


/usr/local/hadoop-0.23.1/bin/hdfs dfs -cat hadoop/remove/p*