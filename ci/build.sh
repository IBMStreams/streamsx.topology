# Requires $HOME/.ant/lib setup correctly - See README.md
. ${WORKSPACE:?}/ci/setup
export PYTHONHOME=${ANACONDA36_HOME:?}
. ${WORKSPACE}/ci/pysetup
echo 'Download JUnit 4.10'
mkdir -p $HOME/.ant/lib
cd $HOME/.ant/lib
wget http://central.maven.org/maven2/junit/junit/4.10/junit-4.10.jar
echo 'Building'
cd $WORKSPACE
ant
