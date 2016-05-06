#!/bin/sh
action=$1
if [ $action = "lib" ]
then
    # python3-config --libs
    :
elif [ $action = "libPath" ]
then
    echo `python3-config --prefix`/lib
elif [ $action = "includePath" ]
then
    python3-config --includes
fi
