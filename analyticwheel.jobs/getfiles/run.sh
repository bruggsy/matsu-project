#!/bin/sh

# We need an input file list string (space delimited) and file list file
# (newline delimited).  This scripts takes the passed in input directory,
# writes a file list, and copies it to HDFS in a pre-determined location
# (filenames.txt)
#
# The input: A path in HDFS
#
# The output: A file containing paths in glusterfs.  That was the key used for
# images when ingest began.  Also, do not include any file extenstion.

# Top-level directory for the run
WORKING_DIR=$1
IN=$2

# Build file list
FILES=`hadoop fs -ls ${IN}`
for f in ${FILES}; do
    # This is the full HDFS path
    echo ${f} | grep -i seqpng  | awk -F".seqpng" '{print $1}' | awk -F"ConvertedImages-hyperion" '{print "/glusterfs/osdc_public_data/eo1/hyperion_l1g"$2}' >> /tmp/out;
done;

# Move file to HDFS
hadoop fs -rm ${WORKING_DIR}/filenames.txt
hadoop fs -copyFromLocal /tmp/out ${WORKING_DIR}/filenames.txt

# Clean up
rm /tmp/out

