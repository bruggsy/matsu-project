#!/bin/bash

# Governing script
# "Wheel of Scripts" to run

# This scripts takes three input parameters, the year, day and top-level day
# directory in HDFS of images to processes.  For example:
#     /user/hadoop/ConvertedImages-hyperion/2013/318
# The "run" becomes all images under that directory.
if [ "$#" -ne 3 ]; then
    echo "  >> Illegal number of parameters."
    echo "  >> Please specify the day-number directory to process (wild cards are allowed),"
    echo "  >> Eg: 2013 318 /user/hadoop/ConvertedImages-hyperion/2013/318"
    echo "  >>      or 2013 318 ConvertedImages-hyperion/2013/318"
    echo "  >> Or a single file,"
    echo "  >> Eg: 2013 001 /user/hadoop/ConvertedImages-hyperion/2013/001/EO1H0881132013001110K4_HYP_L1G.seqpng"
    echo "  >>      or 2013 001 ConvertedImages-hyperion/2013/001/EO1H0881132013001110K4_HYP_L1G.seqpng"
    echo " " 
    echo "  >> This script starts the Analytic Wheel ... " 
    echo " "  

    exit 1
fi

# Value to use with -input in the MapReduce command
YEAR=$1
DAYNO=$2
MR_INPUT=$3


############################ Wheel Turn 1 ######################################
############################## Contours ########################################

BASE_DIR=AnalyticWheelOutput/Contours-2013-12-r3
WORKING_DIR=${BASE_DIR}/${YEAR}-${DAYNO}

#1.0. Set up directory structure, this is under /user/hadoop, which is the
#  implicit working home hdfs directory
hadoop fs -mkdir ${WORKING_DIR}

#1.1.a. Build a file list file for the SummaryStatistics job
/home/hadoop/analyticwheel.jobs/getfiles/run.sh ${WORKING_DIR} ${MR_INPUT}

#1.1.b. Summary Statistics of the images the wheel is run over
# The map reduce input is a file uploaded to HDFS by Step 1.a, instead of MR_INPUT
/home/hadoop/analyticwheel.jobs/summarystatistics/run.sh ${WORKING_DIR} ${MR_INPUT}

#1.2. Contour / Cluster Analytic
/home/hadoop/analyticwheel.jobs/contourclusters/run.sh ${WORKING_DIR} ${MR_INPUT}

#1.3. Apache Report for the Contour / Cluster Analytic
/home/hadoop/analyticwheel.jobs/contours-report/run.sh ${WORKING_DIR} "Contours-2013-12-r3_Noise_Correction=False" ${YEAR} ${DAYNO} "contour-analytic"


############################ Wheel Turn 2 ######################################
####################### Call Another Analytic ##################################

BASE_DIR=AnalyticWheelOutput/Your Directory Here
WORKING_DIR=${BASE_DIR}/${YEAR}-${DAYNO}

#2.0. Set up directory structure, this is under /user/hadoop, which is the
#  implicit working home hdfs directory
hadoop fs -mkdir ${WORKING_DIR}

# Do something ...

exit 0
