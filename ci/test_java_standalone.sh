. $WORKSPACE/ci/setup
echo 'Testing Java standalone'
cd $WORKSPACE/test/java
ant -Dtopology.test.haltonfailure=no -Dtopology.test.threads=`grep -c ^processor /proc/cpuinfo` unittest.standalone
