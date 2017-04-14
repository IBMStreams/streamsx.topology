export PYTHONPATH=$TRAVIS_BUILD_DIR/com.ibm.streamsx.topology/opt/python/packages
cd test/python/topology
python -u -m unittest test2.py
