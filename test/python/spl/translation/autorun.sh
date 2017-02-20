#!/bin/bash

pythonversion=python3
tkpath=../../../../com.ibm.streamsx.topology
languages=( de_DE fr_FR it_IT es_ES pt_BR ja_JP zh_CN ru_RU zh_TW ko_KR en_US )

function usage () {
	command=${0##*/}
	cat <<EOM

This script makes some tests and demonstrated the output in all available languages

	usage: $command [-h]

	options:
		-h | --help 	displayt this help text

	exit status
		0:		test executed
		1:		test fails
EOM
}

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

function errorExit () {
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

#save environment
set +o nounset;
pythonHome=$PYTHONHOME
unset PYTHONHOME
set -o nounset;

for i in "${languages[@]}"; do
	echo "** Test1 $i ********************************************"
	export LC_ALL=$i.UTF-8
	logfile=${logdir}/Test1_$i.log
	echo "output/bin/standalone &> $logfile"
	output/bin/standalone &> $logfile
	echo $logfile
	cat $logfile
done

if [ "$pythonHome" ]; then
	echo "********* restore PYTHON_HOME=$pythonHome *************"
	export PYTHONHOME="$pythonHome"
fi

echo "********************** Test 1 done ********************"

for i in "${languages[@]}"; do
	echo "** Test2 $i ********************************************"
	export LC_ALL=$i.UTF-8
	logfile=${logdir}/Test2_$i.log
	echo "output/bin/standalone -l 2 &> $logfile"
	output/bin/standalone -l 2 &> $logfile || errorExit
	echo $logfile
	cat $logfile
done

echo "********************** Test 2 done ********************"
exit 0
