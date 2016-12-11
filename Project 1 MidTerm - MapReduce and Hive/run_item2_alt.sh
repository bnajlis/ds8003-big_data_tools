#!/bin/bash
#
hdfs dfs -rm /user/root/midterm_item2.out/*
hdfs dfs -rmdir /user/root/midterm_item2.out
rm ./part-00000
hadoop jar /usr/hdp/2.4.0.0-169/hadoop-mapreduce/hadoop-streaming-2.7.1.2.4.0.0-169.jar -file ./midterm_item2_mapper.py -mapper midterm_item2_mapper.py -input /user/root/u.join -output /user/root/midterm_item2.out
hdfs dfs -get /user/root/midterm_item2.out/part-00000