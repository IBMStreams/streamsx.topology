. $WORKSPACE/ci/setup
echo 'Testing Java embedded'
cd $WORKSPACE/test/java
ant -Dtopology.test.threads=`grep -c ^processor /proc/cpuinfo` unittest.main
