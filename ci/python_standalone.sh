unset STREAMS_DOMAIN_ID
unset STREAMS_INSTANCE_ID
unset VCAP_SERVICES
unset STREAMING_ANALYTICS_SERVICE_NAME

#cd $WORKSPACE/test/python/topology
#$PYTHONHOME/bin/python -u -m unittest discover

cd $WORKSPACE/test/python
pyv=`$PYTHONHOME/bin/python -c 'import sys; print(str(sys.version_info.major)+str(sys.version_info.minor))'`
now=`date +%Y%m%d%H%M%S`
xuf="nose_runs/TEST-PY${pyv}_${now}.xml"
export PYTHONPATH=${PYTHONPATH}:${WORKSPACE}/test/python/topology

wd="nose_runs/py${pyv}"
mkdir ${wd}
nosetests --where=${wd} --xunit-file ${xuf} --config=nose.cfg ../../topology
