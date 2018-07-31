. ${WORKSPACE:?}/ci/setup
export PYTHONHOME=${ANACONDA36_HOME:?}
. ${WORKSPACE}/ci/pysetup
echo 'Testing Python 3.6 standalone' 
$WORKSPACE/ci/python_standalone.sh
