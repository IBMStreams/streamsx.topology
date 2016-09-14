#!/bin/sh
# python2 version
action=$1

if [ $action = "lib" ]
then
    python-config --libs | sed -e 's/^-l//;s/ -l/ /g;s/ \+/\n/g'
elif [ $action = "libPath" ]
then
    echo `python-config --prefix`/lib
elif [ $action = "includePath" ]
then
    python-config --includes
fi
