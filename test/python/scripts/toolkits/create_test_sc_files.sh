#!/bin/bash

# Script used/called by test_sc.py to create the info.xml and toolkit.xml files generated for the tests
# Also updates the info.xml and toolkit.xml files with random toolkit names, so different users can run tests concurrently.
# Do NOT call this script from the cmd/terminal

# Path of this file /test/python/scripts/toolkits
PARENT_PATH=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )

# Use the same random name for each toolkit.
NAME=$(head -c16 <(tr -dc '[:lower:]' < /dev/urandom 2>/dev/null))
NAME="com.example.${NAME}"
echo "Random toolkit name is ${NAME}"

# Create info.xml files for the toolkits located in /scripts/toolkits/
for i in $(find ${PARENT_PATH} -name info.xml.tmpl); do
    sed -e "s/com.example./${NAME}./" "$i" > "${i/.tmpl/}"
done

# Creat the toolkit spl folder w/ the random NAME from the template folder
# ie convert tmp.test_tk_1.test1 -> com.example.mcgmufaipldqwcyo.test_tk_1.test1
for i in $(find ${PARENT_PATH} -type d -name 'tmp.test_tk_*.test1'); do
    NEWDIR=${i/tmp/$NAME}
    # echo $NEWDIR
    cp -R $i $NEWDIR
done

# Create the toolkit spl file w/ the random NAME from the template file
# ie create toolkit.spl from toolkit.spl.tmpl
NEWDIR_PATH="${PARENT_PATH}/*/com.example.*.test_tk_*.test1"
for i in $(find $NEWDIR_PATH -name 'toolkit.spl.tmpl'); do
    sed -e "s/test_tk/${NAME}.test_tk/" "$i" > "${i/.tmpl/}"
    rm $i
done

# Create toolkit.xml files for the toolkits located in /scripts/toolkits/
for i in $(find ${PARENT_PATH} -name toolkit.xml.tmpl); do
    sed -e "s/test_tk_/${NAME}.test_tk_/" "$i" > "${i/.tmpl/}"
done

# Create the samplemain spl folder w/ the random NAME from the template folder, located in /scripts/apps/
# ie create samplemain from tmp.samplemain
for i in $(find ${PARENT_PATH}/../apps/ -type d -name 'tmp.samplemain'); do
    NEWDIR=${i/tmp./}
    cp -R $i $NEWDIR
done

# Create the test.spl file w/ the random NAME from the template file, located in /scripts/apps/
# ie create test.spl from test.spl.tmpl
NEWDIR_PATH2="${PARENT_PATH}/../apps/*/samplemain"
# Create toolkit.xml files for the toolkits located in /scripts/toolkits/
for i in $(find $NEWDIR_PATH2 -name test.spl.tmpl); do
    sed -e "s/test_tk/${NAME}.test_tk/" "$i" > "${i/.tmpl/}"
    rm $i
done

# Create info.xml files for the apps, located in /scripts/apps/
for i in $(find ${PARENT_PATH}/../apps/ -name info.xml.tmpl); do
    sed -e "s/com.example./${NAME}./" "$i" > "${i/.tmpl/}"
done