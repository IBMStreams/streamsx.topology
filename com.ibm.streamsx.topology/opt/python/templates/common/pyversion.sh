#!/bin/sh
pythonconfigfile=/tmp/$USER.pythonconfig
pythonconfig=`cat ${pythonconfigfile}`
action=$1

if [ $action = "lib" ]
then
    ${pythonconfig} --libs | sed -e 's/^-l//;s/ -l/ /g;s/ \+/\n/g'
elif [ $action = "libPath" ]
then
    echo `${pythonconfig} --prefix`/lib
elif [ $action = "includePath" ]
then
    ${pythonconfig} --includes
fi
