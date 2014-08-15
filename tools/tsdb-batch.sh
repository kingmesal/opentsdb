#!/bin/bash

JAR_PATH="/opt/jscott/opentsdb-2.0.0.jar"

getDir() {
	# Absolute path to this script, e.g. /home/user/bin/foo.sh
	SCRIPT=`readlink -f $0`
	# Absolute path this script is in, thus /home/user/bin
	SCRIPTPATH=`dirname $SCRIPT`
	echo $SCRIPTPATH
}

if [ $# -lt 6 ]; then
	echo "Usage: $0 YEAR JVM_COUNT THREAD_COUNT METRICS_NAME UNIQUE_METRICS FLUSH_INTERVAL"
	echo "	e.g. $0 1970 8 2 pills 8 1000)"
	echo "	produces 8 jvms, each running two threads, using the pills list for metric names"
	echo "	with 8 variations of each metric name and flushing to the datastore every 1000ms"
	exit
fi

# First year to use, how many years total should it iterate through?
START_YEAR=$1
YEARS=1

# For each JVM increment the job id for the next job
TOTAL_JVMS=$2
JOB_ID=0

# Total number of internal threads the JVM should use while processing the metrics and years
THREAD_COUNT=$3

# Read the name of the metrics to use from the command line
NAMES=$4

# How many variations of each metric?
MULTI=$5

# How many millis before the JVM should flush to MapR-DB
FLUSH_INTERVAL=$6

# To test with an individual put for each data point use "single" instead of "batch"
MODE="batch"

#
# Beginning of the logic
# READ the file to get the metrics to use for the jobs
#
COMMON_DIR=$(getDir)
source $COMMON_DIR/metric-names-$NAMES.sh

for metric in $METRICS
do
	# Only produce as many jobs as the JVMs limit requested
	((JOB_ID++))
	if [ $JOB_ID -le $TOTAL_JVMS ]; then
		COMMAND="/usr/bin/nohup /usr/bin/java -Xmx3G -jar $JAR_PATH --action=BULK --importMode=$MODE --flushInterval=$FLUSH_INTERVAL --year=$START_YEAR --totalYears=$YEARS --threads=$THREAD_COUNT --multiMetric=$MULTI --jobId=$JOB_ID --metric=$metric"

		echo "$COMMAND"
		$COMMAND > /tmp/tsdb-batch.$JOB_ID.out &
	fi
done

