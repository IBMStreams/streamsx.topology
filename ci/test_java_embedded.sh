. ${WORKSPACE:?}/ci/setup
# Build requires a Python 3.5/6 setup.
export PYTHONHOME=${ANACONDA36_HOME:?}
. ${WORKSPACE}/ci/pysetup
echo 'Testing Java embedded'
cd $WORKSPACE/test/java
ant -Dtopology.test.haltonfailure=no -Dtopology.test.threads=${CI_TEST_THREADS:-8} unittest.main
