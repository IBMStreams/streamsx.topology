unset STREAMS_DOMAIN_ID
unset STREAMS_INSTANCE_ID
unset VCAP_SERVICES
unset STREAMING_ANALYTICS_SERVICE_NAME

cd $WORKSPACE/test/python/topology
$PYTHONHOME/bin/python -u -m unittest discover
