. ${WORKSPACE:?}/ci/setup
export PYTHONHOME=${ANACONDA35_HOME:?}
. ${WORKSPACE}/ci/pysetup
echo 'Testing Python 3.5 standalone' 
$WORKSPACE/ci/python_standalone.sh
