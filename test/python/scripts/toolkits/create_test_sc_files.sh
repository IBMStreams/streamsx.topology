#!/bin/bash

# Script used/called by test_sc.py to create the info.xml and toolkit.xml files generated for the tests
# Also updates the info.xml and toolkit.xml files with random toolkit names, so different users can run tests concurrently.
# Do NOT call this script from the cmd/terminal


# Use the same random name for each toolkit.
NAME=$(head -c16 <(tr -dc '[:lower:]' < /dev/urandom 2>/dev/null))
echo "Random toolkit name is ${NAME}"

# Create info.xml files for the toolkits located in /scripts/toolkits/
cd toolkits
for i in $(find . -name info.xml.tmpl); do
    sed -e "s/com.example./com.example.${NAME}./" "$i" > "${i/.tmpl/}"
done

# Create toolkit.xml files for the toolkits located in /scripts/toolkits/
for i in $(find . -name toolkit.xml.tmpl); do
    sed -e "s/com.example./com.example.${NAME}./" "$i" > "${i/.tmpl/}"
done


# Create info.xml files for the apps, located in /scripts/apps/
cd ../apps/
for i in $(find . -name info.xml.tmpl); do
    sed -e "s/com.example./com.example.${NAME}./" "$i" > "${i/.tmpl/}"
done