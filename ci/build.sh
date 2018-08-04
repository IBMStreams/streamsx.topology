# Requires $HOME/.ant/lib setup correctly - See README.md
. ${WORKSPACE:?}/ci/setup
export PYTHONHOME=${ANACONDA36_HOME:?}
. ${WORKSPACE}/ci/pysetup
echo 'Building'
cd $WORKSPACE
ant
