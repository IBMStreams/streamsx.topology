#!/bin/bash

# Script used/called by test_sc.py to delete the info.xml and toolkit.xml files generated for the tests
# Also updates the info.xml and toolkit.xml files with random toolkit names, so different users can run tests concurrently.
# Do NOT call this script from the cmd/terminal

# Delete info.xml files for the toolkits located in /scripts/toolkits/
cd toolkits
for i in $(find . -name info.xml); do
    rm $i
done

# Delete toolkit.xml files for the toolkits located in /scripts/toolkits/
for i in $(find . -name toolkit.xml); do
    rm $i
done

# Delete info.xml files for the apps, located in /scripts/apps/
cd ../apps/
for i in $(find . -name info.xml); do
    rm $i
done