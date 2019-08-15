. ${WORKSPACE:?}/ci/setup

export PYTHONHOME=${ANACONDA36_HOME:?}
. ${WORKSPACE}/ci/pysetup
echo 'Testing Python 3.6 Streaming Analytics service' 

$WORKSPACE/ci/python_service.sh

