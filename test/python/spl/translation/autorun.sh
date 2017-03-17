#!/bin/bash

set -o errexit

pythonversion=python3
tkpath=../../../../com.ibm.streamsx.topology
languages=( de_DE fr_FR it_IT es_ES pt_BR ja_JP zh_CN ru_RU zh_TW ko_KR en_US )

function usage () {
	command=${0##*/}
	cat <<EOM

This script makes some tests and demonstrated the output in all available languages
The script runs in Streams versions >= 4.2
The test script is made to support the translation verification test. It generates 3 log-messages in all available languages.
These messages may be used to verify the translation.
So it is currently nor suited for automated test

	usage: $command [-h]

	options:
		-h | --help 	displayt this help text

	exit status
		0:		all tests successfully executed
		1:		fatal error
		20:		Streams version is not supported
		30:		at least one test fails
EOM
}
vers=$(${STREAMS_INSTALL}/bin/streamtool version | grep Version)
if [[ ${vers} =~ Version=([0-9])\.([0-9])\.([0-9]) ]]; then
	if [[ ${BASH_REMATCH[1]} -lt 4 || ${BASH_REMATCH[2]} -lt 2 ]]; then
		echo "The Streams version ${vers} is not supported from this test tool. Use at least Streams V4.2. Abort execution!"
		exit 0
	fi
else
	echo "Invalid version string ${vers} . Abort execution!"
	exit 1
fi

while getopts ":h" opt; do
	case $opt in
	h)
		usage $0
		exit 0
		;;
	\?)
		echo "Wrong option $OPTARG"
		usage
		exit 1
		;;
	esac
done

set -o nounset;

declare -i failures=0
declare -i executed=0


function errorExit () {
	echo "########################## error exiting ##################" 1>&2
	echo "Executed=$executed Errors=1 Failures=$failures" 1>&2
	echo "########################## error exiting ##################" 1>&2
	exit 1
}

nproc=$(grep processor /proc/cpuinfo | wc -l)
logdir=`date +logs/%Y%m%d-%H%M%S`
mkdir -p $logdir

#Make sample
echo "***************** Generate spl operators from python sample code *******************"
cmd="${pythonversion} ${tkpath}/bin/spl-python-extract.py -i ."
echo $cmd
$cmd || errorExit
echo "***************** Compile spl artifacts ********************************************"
cmd="${STREAMS_INSTALL}/bin/sc -M testspl::NoopSample -j ${nproc} -t ${tkpath} -C"
echo $cmd
$cmd || errorExit
cmd="${STREAMS_INSTALL}/bin/sc -M testspl::NoopSample -j ${nproc} -t ${tkpath}"
echo $cmd
$cmd || errorExit

#Test 1 Can not execute

#save original environment and manipulate the environment so that the standalon will fail
set +o nounset;
pythonHome=$PYTHONHOME
unset PYTHONHOME
set -o nounset;

for i in "${languages[@]}"; do
	executed=$((executed+1))
	echo "** Test1 $i ********************************************"
	export LC_ALL=$i.UTF-8
	logfile=${logdir}/Test1_$i.log
	echo "output/bin/standalone &> $logfile"
	#we expect that the standalone execution fails
	if output/bin/standalone &> $logfile; then
		failures=$((failures+1))
		echo "FAILURE: Test 1 lang $i failed" 1>&2
	fi
	echo $logfile
	cat $logfile
done

if [ "$pythonHome" ]; then
	echo "********* restore PYTHON_HOME=$pythonHome *************"
	export PYTHONHOME="$pythonHome"
fi

echo "********************** Test 1 done ********************"

for i in "${languages[@]}"; do
	executed=$((executed+1))
	echo "** Test2 $i ********************************************"
	export LC_ALL=$i.UTF-8
	logfile=${logdir}/Test2_$i.log
	echo "output/bin/standalone -l 2 &> $logfile"
	#we expect that the standalone execution succeeds
	if ! output/bin/standalone -l 2 &> $logfile; then
		failures=$((failures+1))
		echo "FAILURE: Test 2 lang $i failed" 1>&2
	fi
	echo $logfile
	cat $logfile
done

echo "********************** Test 2 done ********************"

if (( failures == 0 )); then
	echo "*********************************************************"
	echo "Executed=$executed Errors=0 Failures=$failures"
	echo "Successfully completed"
	echo "*********************************************************"
	exit 0
else
	echo "*********************************************************"
	echo "Executed=$executed Errors=0 Failure=$failures"
	echo "FAILURE: Completed with ${failures} failures" 1>&2
	echo "*********************************************************"
	exit 30
fi;
