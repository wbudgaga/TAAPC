#!/bin/bash

if [ "$1" != "" ]; then

     /usr/local/hadoop-0.23.1/bin/hdfs dfs -rm -r  hadoop/bigrams1*
     /usr/local/hadoop-0.23.1/bin/hadoop jar ./cs455.jar cs455.BigramCreator hadoop/books hadoop/bigrams1

fi

/usr/local/hadoop-0.23.1/bin/hdfs dfs -rm -r  hadoop/addition 


/usr/local/hadoop-0.23.1/bin/hadoop jar ./cs455.jar cs455.FindMissingWord hadoop/probabilities hadoop/addition


/usr/local/hadoop-0.23.1/bin/hdfs dfs -cat hadoop/addition/p*