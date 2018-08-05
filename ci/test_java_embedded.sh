. ${WORKSPACE:?}/ci/setup
echo 'Testing Java embedded'
cd $WORKSPACE/test/java
ant -Dtopology.test.haltonfailure=no -Dtopology.test.threads=${CI_TEST_THREADS:-8} unittest.main
