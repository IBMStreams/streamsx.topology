# Requires $HOME/.ant/lib setup correctly - See README.md
. ${WORKSPACE:?}/ci/setup
echo 'Download JUnit 4.10'
echo 'Building'
cd $WORKSPACE
ant
