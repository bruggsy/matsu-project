#!/bin/bash
# run.sh for the summary startistics job


local_work_dir=/home/hadoop/analyticwheel.jobs/summarystatistics
libjar_dir=${local_work_dir}/../lib/GetFromAccumulo/target

WORKING_DIR=$1
# Included for consistency and is not used.  For this step, we look at filenames.txt
MR_INPUT=$2

hadoop jar ${HADOOP_HOME}/contrib/streaming/hadoop-streaming-*.jar \
 -D mapred.reduce.tasks=1 \
 -D accumulotablename=SummaryStatistics_hyperion \
 -libjars ${libjar_dir}/getfromaccumulo-0.1-SNAPSHOT-job.jar,${libjar_dir}/lib/commons-collections-3.2.jar,${libjar_dir}/lib/commons-codec-1.4.jar,${libjar_dir}/lib/commons-lang-2.4.jar,${libjar_dir}/lib/commons-io-1.4.jar,${libjar_dir}/lib/servlet-api-2.5.jar,${libjar_dir}/lib/junit-4.4.jar,${libjar_dir}/lib/jline-0.9.94.jar,${libjar_dir}/lib/commons-logging-api-1.0.4.jar,${libjar_dir}/lib/commons-logging-1.0.4.jar,${libjar_dir}/lib/log4j-1.2.16.jar,${libjar_dir}/lib/commons-jci-core-1.0.jar,${libjar_dir}/lib/cloudtrace-1.4.1.jar,${libjar_dir}/lib/accumulo-start-1.4.1.jar,${libjar_dir}/lib/accumulo-core-1.4.1.jar,${libjar_dir}/lib/httpcore-4.0.1.jar,${libjar_dir}/lib/httpclient-4.0.1.jar,${libjar_dir}/lib/commons-jci-fam-1.0.jar,${libjar_dir}/lib/slf4j-log4j12-1.4.3.jar,${libjar_dir}/lib/slf4j-api-1.4.3.jar,${libjar_dir}/lib/libthrift-0.6.1.jar,${libjar_dir}/lib/zookeeper-3.3.3.jar \
 -input ${WORKING_DIR}/filenames.txt \
 -output ${WORKING_DIR}/summarystats \
 -mapper com.opendatagroup.hyperspectral.getfromaccumulo.AccumuloMapper \
 -reducer ${local_work_dir}/SummaryStatisticsReducer.py \
 -file ${local_work_dir}/SummaryStatisticsReducer.py \
 -file ${local_work_dir}/../bin/config

exit 0
