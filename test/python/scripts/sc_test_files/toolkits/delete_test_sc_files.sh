#!/bin/bash

# Script used/called by test_sc.py to delete the info.xml and toolkit.xml files generated for the tests
# Do NOT call this script from the cmd/terminal

# Path of this file /test/python/scripts/sc_test_files/toolkits
PARENT_PATH=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )

# Delete generated info.xml files for the toolkits located in /scripts/sc_test_files/toolkits/
find ${PARENT_PATH} -name info.xml | xargs rm -f

# Delete generated com.example.*.test_tk_*.test1 folder
find $PARENT_PATH -type d -name 'com.example.*.test_tk_*.test1' | xargs rm -rf

# Delete generated toolkit.xml files for the toolkits located in /scripts/sc_test_files/toolkits/
find ${PARENT_PATH} -name toolkit.xml | xargs rm -f

# Delete generated samplemain folder, located in /scripts/sc_test_files/apps/
find ${PARENT_PATH}/../apps/ -type d -name samplemain | xargs rm -rf

# Delete generated info.xml files for the apps, located in /scripts/sc_test_files/apps/
find ${PARENT_PATH}/../apps/ -name info.xml | xargs rm -f
