. ${WORKSPACE:?}/ci/setup
export PYTHONHOME=${ANACONDA27_HOME:?}
. ${WORKSPACE}/ci/pysetup
${PYTHONHOME}/bin/python -m pip install funcsigs
echo 'Testing Python 2.7 standalone' 
$WORKSPACE/ci/python_standalone.sh
