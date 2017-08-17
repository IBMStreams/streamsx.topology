set -e

echo JAVA_HOME=$JAVA_HOME
java -version
J=`which javac`
JH1=`dirname $J`
JH2=`dirname $JH1`

export JAVA_HOME=$JH2
echo JAVA_HOME=$JAVA_HOME


cd $TRAVIS_BUILD_DIR/java
export STREAMS_INSTALL=/dev/null
ant compile.pure

exit 0

# TBD - Work in progress
unset STREAMS_INSTALL

cd $TRAVIS_BUILD_DIR/test/python/topology
export PYTHONPATH=$TRAVIS_BUILD_DIR/com.ibm.streamsx.topology/opt/python/packages
python -u -m unittest test2.py
