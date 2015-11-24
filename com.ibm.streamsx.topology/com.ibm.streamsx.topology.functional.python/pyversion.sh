#!/bin/sh
action=$1
if [ $action = "lib" ]
then
    :
elif [ $action = "libPath" ]
then
    :
elif [ $action = "includePath" ]
then
    python3-config --includes
fi
