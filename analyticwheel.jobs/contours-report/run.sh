#!/bin/bash 

WORKING_DIR=$1
ANALYTIC_NAME=$2
NOISE_CORRECTION=$3
YEAR=$4
DAY=$5
ANALYTIC_APACHE_DIR=$6

LOCAL_DIR=/home/hadoop/analyticwheel.jobs/contours-report


function getMonth() {
# This takes a year and day of year and returns the two-digit month it belongs
# to as a string.  Ex. 07

  LYA=0
  LEAPYEARS="1972 1976 1980 1984 1988 1992 1996 2000 2004 2008 2012 2016 2020"
  for Y in ${LEAPYEARS} ; do
    if [ ${YEAR} == $Y ] ; then 
        LYA=1
        break
    fi
  done


  if [ ${DAY} -lt 32 ] ; then
     MONTH="01"
  elif [ ${DAY} -lt `expr 60 + $LYA` ] ; then
     MONTH="02"
  elif [ ${DAY} -lt `expr 91 + $LYA` ] ; then
     MONTH="03"
  elif [ ${DAY} -lt `expr 121 + $LYA` ] ; then
     MONTH="04"
  elif [ ${DAY} -lt `expr 152 + $LYA` ] ; then
     MONTH="05"
  elif [ ${DAY} -lt `expr 182 + $LYA` ] ; then
     MONTH="06"
  elif [ ${DAY} -lt `expr 213 + $LYA` ] ; then
     MONTH="07"
  elif [ ${DAY} -lt `expr 244 + $LYA` ] ; then
     MONTH="08"
  elif [ ${DAY} -lt `expr 274 + $LYA` ] ; then
     MONTH="09"
  elif [ ${DAY} -lt `expr 305 + $LYA` ] ; then
     MONTH="10"
  elif [ ${DAY} -lt `expr 335 + $LYA` ] ; then
     MONTH="11"
  else
     MONTH="12"
  fi

  echo ${MONTH}
}


TEMP=`mktemp -d`
hadoop fs -get ${WORKING_DIR}/contours-report ${TEMP}
MONTH=`getMonth`

python ${LOCAL_DIR}/build-image-report.py   ${YEAR} ${DAY} ${ANALYTIC_NAME} ${TEMP}/contours-report/part-00000 ${TEMP} ${NOISE_CORRECTION}
python ${LOCAL_DIR}/build-summary-report.py ${YEAR} ${DAY} ${ANALYTIC_NAME} ${TEMP}/contours-report/part-00000 ${TEMP} ${NOISE_CORRECTION}

# Check that the target diretory exists
mkdir -p /var/www/reports/${YEAR}-${MONTH}/${ANALYTIC_APACHE_DIR}/overlays/

# Copy the files
cp ${TEMP}/image-*.html /var/www/reports/${YEAR}-${MONTH}/${ANALYTIC_APACHE_DIR}/overlays/
cp ${TEMP}/spectral-*.html /var/www/reports/${YEAR}-${MONTH}/${ANALYTIC_APACHE_DIR}/overlays/
cp ${TEMP}/summary-*.html /var/www/reports/${YEAR}-${MONTH}/${ANALYTIC_APACHE_DIR}/

rm -rf ${TEMP}

