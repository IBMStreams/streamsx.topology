#!/bin/bash

# Script used/called by test_sc.py to create the info.xml and toolkit.xml files generated for the tests
# Also updates the info.xml and toolkit.xml files with random toolkit names, so different users can run tests concurrently.
# Do NOT call this script from the cmd/terminal

# Path of this file /test/python/scripts/toolkits
PARENT_PATH=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )

# Use the same random name for each toolkit.
NAME=$(head -c16 <(tr -dc '[:lower:]' < /dev/urandom 2>/dev/null))
echo "Random toolkit name is ${NAME}"

# Create info.xml files for the toolkits located in /scripts/toolkits/
for i in $(find ${PARENT_PATH} -name info.xml.tmpl); do
    sed -e "s/com.example./com.example.${NAME}./" "$i" > "${i/.tmpl/}"
done

# Create toolkit.xml files for the toolkits located in /scripts/toolkits/
for i in $(find ${PARENT_PATH} -name toolkit.xml.tmpl); do
    sed -e "s/com.example./com.example.${NAME}./" "$i" > "${i/.tmpl/}"
done

# Create info.xml files for the apps, located in /scripts/apps/
for i in $(find ${PARENT_PATH}/../apps/ -name info.xml.tmpl); do
    sed -e "s/com.example./com.example.${NAME}./" "$i" > "${i/.tmpl/}"
done