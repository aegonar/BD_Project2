# Big Data Project 2

## HDFS, Hive, Spark, SparkSQL

Alejandro Gonzalez
Fall 2018

Set Twitter API credentials on credentials.json
Run Twitter Streamer with: watchdog.sh
This calls streamer.py and lets it restart on failure

Preprocess data with bdp2-0.0.1-SNAPSHOT-shaded.jar
Data is stored at root, merge with merge.sh

Store data at hdfs with hdfs_put.sh

Start Spark hourly processing with sparkprocess.py

Some tips at howto file
