unset STREAMS_DOMAIN_ID
unset STREAMS_INSTANCE_ID
unset VCAP_SERVICES
unset STREAMING_ANALYTICS_SERVICE_NAME

set -x

# Clean the extracted toolkits to avoid artifacts from
# previous runs with different Python versions.
cd $WORKSPACE/test/python/spl
find testtkpy* -name 'toolkit.xml' | xargs rm

cd $WORKSPACE/test/python
pyv=`$PYTHONHOME/bin/python -c 'import sys; print(str(sys.version_info.major)+str(sys.version_info.minor))'`
now=`date +%Y%m%d%H%M%S`
xuf="nose_runs/TEST-PY${pyv}_${now}.xml"
export PYTHONPATH=${PYTHONPATH}:${WORKSPACE}/test/python/topology:${WORKSPACE}/test/python/spl/tests:

wd="$WORKSPACE/test/python/nose_runs/py${pyv}"
mkdir -p ${wd}
# xunit not working with processes
# --xunit-file ${xuf} --xunit-testsuite-name="py${pyv}" 
nosetests --where=${wd} --config=nose.cfg ../../topology ../../spl/tests
nrc=$?
# Ensure only test failures just cause an unstable build.
# Disabled for now since with multiple processes xunit does not
# collect results correctly
#if [ ${nrc} -eq 1 ] && [ -e  ${xuf} ]; then exit 0; else exit ${nrc}; fi
exit ${nrc}
