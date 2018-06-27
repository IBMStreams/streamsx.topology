# Requires $HOME/.ant/lib setup correctly - See README.md
. $WORKSPACE/ci/setup
echo 'Building'
cd $WORKSPACE
ant
