#!/bin/bash
#run.sh
# Expects a path name that specifies 
# at least one output directory from AnalyticWheelOutput/YEAR/DAY/contours/*
# run as: ./run.sh /PATH/TO/AnalyticWheelOutput/YEAR/DAY/contours/*, or
#         ./run.sh ?PATH/TO/AnalyticWheelOutput/YEAR/[0123]*/contours/*

# values to use in the scripted steps below
local_work_dir=/home/hadoop/analyticwheel.jobs/contourclusters-r3/reporting

# Score output directory
score_info_dir=/home/hadoop/AnalyticWheelOutput/score-map

################
###SCORE-MAP####

hadoop jar ${HADOOP_HOME}/contrib/streaming/hadoop-streaming-*.jar \
 -D mapred.reduce.tasks=0 \
 -D mapred.min.split.size=10737418240 \
 -input $1 \
 -output ${score_info_dir} \
 -mapper ${local_work_dir}/makeQuantileScores.py \
 -reducer NONE \
 -file ${local_work_dir}/makeQuantileScores.py 

exit 0
