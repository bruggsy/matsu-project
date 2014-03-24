#!/bin/bash

# values to use in the scripted steps below
local_work_dir=../contourclusters
# Hadoop streaming bug workaround
libjar_dir=/home/hadoop/analyticwheel.jobs/lib/PutToAccumulo/target
# HDFS cache used to pass around files
CACHE=hdfs://node:port


WORKING_DIR=$1
MR_INPUT=$2

# Intermediate working directories
pca_output_dir=${WORKING_DIR}/temp/contour-pca-1
pca_aggregate_dir=${WORKING_DIR}/temp/contour-pca-agg
projected_image_dir=${WORKING_DIR}/temp/contour-proj

# Built in a previous MR step
summarystats_dir=${WORKING_DIR}/summarystats
# Output directories to keep
analyzed_image_dir=${WORKING_DIR}/contours
final_image_dir=${WORKING_DIR}/contours-report


################
###PCA##########

hadoop jar ${HADOOP_HOME}/contrib/streaming/hadoop-streaming-*.jar \
 -D stream.map.output=typedbytes \
 -D stream.reduce.input=typedbytes \
 -D mapred.min.split.size=10737418240 \
 -inputformat org.apache.hadoop.mapred.SequenceFileAsBinaryInputFormat \
 -input ${MR_INPUT} \
 -output ${pca_output_dir} \
 -mapper ${local_work_dir}/pca/hsi_pca_mapper.py \
 -reducer ${local_work_dir}/pca/hsi_pca_reducer.py \
 -file ${local_work_dir}/pca/hsi_pca_mapper.py \
 -file ${local_work_dir}/pca/hsi_pca_reducer.py \
 -file ${local_work_dir}/../lib/NewImageScan/src/main/python/binaryhadoop.py \
 -file ${local_work_dir}/../lib/NewImageScan/src/main/python/utilities.py \
 -file ${local_work_dir}/analyticconfig

################
##AGGREGATE#####

hadoop jar ${HADOOP_HOME}/contrib/streaming/hadoop-streaming-*.jar \
 -D mapred.reduce.tasks=0 \
 -D mapred.min.split.size=10737418240 \
 -input ${pca_output_dir}/part-* \
 -output ${pca_aggregate_dir} \
 -mapper ${local_work_dir}/aggregation/hsi_pca_agg_mapper.py \
 -reducer NONE \
 -file ${local_work_dir}/aggregation/hsi_pca_agg_mapper.py \
 -file ${local_work_dir}/../lib/NewImageScan/src/main/python/binaryhadoop.py 


#################
###PROJECTION####

hadoop jar ${HADOOP_HOME}/contrib/streaming/hadoop-streaming-*.jar \
 -D mapred.reduce.tasks=0 \
 -D stream.map.output=typedbytes \
 -D stream.reduce.input=typedbytes \
 -D stream.reduce.output=typedbytes \
 -D mapred.min.split.size=10737418240 \
 -files ${CACHE}/user/hadoop/${pca_aggregate_dir}/part-00000 \
 -inputformat org.apache.hadoop.mapred.SequenceFileAsBinaryInputFormat \
 -outputformat org.apache.hadoop.mapred.SequenceFileOutputFormat \
 -input ${MR_INPUT} \
 -output ${projected_image_dir} \
 -mapper ${local_work_dir}/projection/hsi_projection_mapper.py \
 -reducer NONE \
 -file ${local_work_dir}/projection/hsi_projection_mapper.py \
 -file ${local_work_dir}/../lib/NewImageScan/src/main/python/utilities.py \
 -file ${local_work_dir}/../lib/NewImageScan/src/main/python/binaryhadoop.py \
 -file ${local_work_dir}/analyticconfig

#################
####ANALYTIC#####

hadoop jar ${HADOOP_HOME}/contrib/streaming/hadoop-streaming-*.jar \
 -D mapred.reduce.tasks=1 \
 -D stream.map.output=typedbytes \
 -D stream.reduce.input=typedbytes \
 -D mapred.min.split.size=10737418240 \
 -D accumulotablename=Analytic_ClusterContours \
 -libjars ${libjar_dir}/puttoaccumulo-0.1-SNAPSHOT-job.jar,${libjar_dir}/lib/commons-collections-3.2.jar,${libjar_dir}/lib/commons-codec-1.4.jar,${libjar_dir}/lib/commons-lang-2.4.jar,${libjar_dir}/lib/commons-io-1.4.jar,${libjar_dir}/lib/servlet-api-2.5.jar,${libjar_dir}/lib/junit-4.4.jar,${libjar_dir}/lib/jline-0.9.94.jar,${libjar_dir}/lib/commons-logging-api-1.0.4.jar,${libjar_dir}/lib/commons-logging-1.0.4.jar,${libjar_dir}/lib/log4j-1.2.16.jar,${libjar_dir}/lib/commons-jci-core-1.0.jar,${libjar_dir}/lib/cloudtrace-1.4.1.jar,${libjar_dir}/lib/accumulo-start-1.4.1.jar,${libjar_dir}/lib/accumulo-core-1.4.1.jar,${libjar_dir}/lib/httpcore-4.0.1.jar,${libjar_dir}/lib/httpclient-4.0.1.jar,${libjar_dir}/lib/commons-jci-fam-1.0.jar,${libjar_dir}/lib/slf4j-log4j12-1.4.3.jar,${libjar_dir}/lib/slf4j-api-1.4.3.jar,${libjar_dir}/lib/libthrift-0.6.1.jar,${libjar_dir}/lib/zookeeper-3.3.3.jar,${libjar_dir}/lib/seqpng-0.1-SNAPSHOT.jar \
 -inputformat org.apache.hadoop.mapred.SequenceFileAsBinaryInputFormat \
 -input ${projected_image_dir} \
 -output ${analyzed_image_dir}\
 -mapper ${local_work_dir}/analyticspectral/hsi_kmeans_anomaly_mapper.py \
 -reducer com.opendatagroup.hyperspectral.puttoaccumulo.JsonToAccumuloReducer \
 -file ${local_work_dir}/analyticspectral/hsi_kmeans_anomaly_mapper.py \
 -file ${local_work_dir}/../lib/NewImageScan/src/main/python/utilities.py \
 -file ${local_work_dir}/../lib/NewImageScan/src/main/python/binaryhadoop.py \
 -file ${local_work_dir}/analyticconfig \
 -file ${local_work_dir}/../bin/config

#################
###IMAGING####

hadoop jar ${HADOOP_HOME}/contrib/streaming/hadoop-streaming-*.jar \
 -D mapred.min.split.size=10737418240 \
 -D stream.num.map.output.key.fields=1 \
 -files ${CACHE}/user/hadoop/${analyzed_image_dir}/part-00000#clustercontours,${CACHE}/user/hadoop/${summarystats_dir}/part-00000#summarystatistics \
 -inputformat org.apache.hadoop.mapred.SequenceFileAsBinaryInputFormat \
 -input ${MR_INPUT} \
 -output ${final_image_dir} \
 -mapper ${local_work_dir}/reporting/hsi_create_image_mapper.py \
 -reducer ${local_work_dir}/reporting/report.py \
 -file ${local_work_dir}/reporting/hsi_create_image_mapper.py \
 -file ${local_work_dir}/reporting/report.py \
 -file ${local_work_dir}/reporting/rasterAndPolygonsToSvg.py \
 -file ${local_work_dir}/../lib/NewImageScan/src/main/python/utilities.py \
 -file ${local_work_dir}/../lib/NewImageScan/src/main/python/binaryhadoop.py \
 -file ${local_work_dir}/analyticconfig \
 -file ${local_work_dir}/normconfig

# Clean up 
hadoop fs -rmr -skipTrash ${WORKING_DIR}/temp

exit 0

