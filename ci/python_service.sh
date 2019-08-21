
unset STREAMS_INSTALL
unset STREAMS_DOMAIN_ID
unset STREAMS_INSTANCE_ID

# Don't assume a service exists for testing.
if [ -z "${VCAP_SERVICES}" ] || [ -z "STREAMING_ANALYTICS_SERVICE_NAME" ]; then exit 0; fi

# Check the environment is correct
echo "${VCAP_SERVICES:?}" > /dev/null
echo "Service name: "  ${STREAMING_ANALYTICS_SERVICE_NAME:?}

set -x

cd $WORKSPACE/test/python
pyv=`$PYTHONHOME/bin/python -c 'import sys; print(str(sys.version_info.major)+str(sys.version_info.minor))'`
now=`date +%Y%m%d%H%M%S`
xuf="nose_runs/TEST-SERVICE-PY${pyv}_${now}.xml"
export PYTHONPATH=${PYTHONPATH}:${WORKSPACE}/test/python/topology:${WORKSPACE}/test/python/spl/tests:${WORKSPACE}/test/python/rest

wd="$WORKSPACE/test/python/nose_runs/service-py${pyv}"
mkdir -p ${wd}
# xunit not working with processes
# --xunit-file ${xuf} --xunit-testsuite-name="pysvc${pyv}"
nosetests --where=${wd} --config=nose.cfg --with-streamsx-skip-standalone ../../rest
nrc=$?

# Ensure only test failures just cause an unstable build.
# Disabled for now since with multiple processes xunit does not
# collect results correctly
#if [ ${nrc} -eq 1 ] && [ -e  ${xuf} ]; then exit 0; else exit ${nrc}; fi
exit ${nrc}
