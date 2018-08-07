# Requires $HOME/.ant/lib setup correctly - See README.md
. ${WORKSPACE:?}/ci/setup
# Build requires a Python 3.5/6 setup.
export PYTHONHOME=${ANACONDA36_HOME:?}
. ${WORKSPACE}/ci/pysetup
echo 'Building'
cd $WORKSPACE
ant
