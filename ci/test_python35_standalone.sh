. ${WORKSPACE:?}/ci/setup

# 3.5 environment is assumed to be conda environment created from 3.6
source ${ANACONDA36_HOME:?}/bin/activate `basename ${ANACONDA35_HOME}`
export PYTHONHOME=${ANACONDA35_HOME:?}
. ${WORKSPACE}/ci/pysetup
echo 'Testing Python 3.5 standalone' 
$WORKSPACE/ci/python_standalone.sh
prc=$?
source ${ANACONDA36_HOME:?}/bin/deactivate `basename ${ANACONDA35_HOME}`
exit ${prc}
