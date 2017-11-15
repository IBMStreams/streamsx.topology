#!/bin/bash

export STREAMING_ANALYTICS_SERVICE_NAME=MyStreamingAnalytics
export VCAP_SERVICES=$( cat $HOME/vcap.json )

#python3 -m unittest -v string_tests
python3 -m unittest -v submit_tests

exit $?