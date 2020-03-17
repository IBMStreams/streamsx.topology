#!/bin/bash

unset STREAMS_INSTALL
unset STREAMS_DOMAIN_ID


[[ -z "$CP4D_URL" ]] && { echo "Error: CP4D_URL not found in environment"; exit 1; }
[[ -z "$STREAMS_INSTANCE_ID" ]] && { echo "Error: STREAMS_INSTANCE_ID not found in environment"; exit 1; }
[[ -z "$STREAMS_USERNAME" ]] && { echo "Error: STREAMS_USERNAME not found in environment"; exit 1; }
[[ -z "$STREAMS_PASSWORD" ]] && { echo "Error: STREAMS_PASSWORD not found in environment"; exit 1; }

echo "--------------------"
echo "Testing rest ..."
cd rest
python -u -m unittest test_toolkits.TestDistributedRestToolkitAPI
cd -

echo "--------------------"
echo "Testing scripts ..."
cd scripts
python3 -u -m unittest test_st_appconfig.py -v
cd -

echo "--------------------"
echo "Testing spl/tests ..."
cd spl/tests
python3 -u -m unittest test_splpy_exceptions.TestDistributedSuppressMetric -v
cd -

echo "--------------------"
echo "Testing topology ..."
cd topology
python3 -u -m unittest test2.TestDistributedTopologyMethodsNew test2_ec.TestDistributedEc test2_pending.TestDistributedPending test2_pubsub.TestDistributedPubSub test2_spl2python.TestDistributedSPL test2_views.TestDistributedViews -v
#test2_checkpoint.TestDistributedCheckpointing test2_consistent.TestDistributedConsistentRegion test2_udp.TestDistributedUDP
cd -


