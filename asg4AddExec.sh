#!/bin/bash

if [ "$1" != "" ]; then

     /usr/local/hadoop-0.23.1/bin/hdfs dfs -rm -r  hadoop/bigrams* hadoop/probabilities*
     /usr/local/hadoop-0.23.1/bin/hadoop jar ./cs455.jar cs455.BigramCreator -r 40 hadoop/books hadoop/bigrams
	/usr/local/hadoop-0.23.1/bin/hadoop jar ./cs455.jar cs455.ProbabilityCalculator hadoop/bigrams hadoop/probabilities
fi

/usr/local/hadoop-0.23.1/bin/hdfs dfs -rm -r  hadoop/addition 


/usr/local/hadoop-0.23.1/bin/hadoop jar ./cs455.jar cs455.AddMissingWord hadoop/probabilities hadoop/addition


/usr/local/hadoop-0.23.1/bin/hdfs dfs -cat hadoop/addition/p*