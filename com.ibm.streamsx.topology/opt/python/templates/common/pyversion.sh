#!/bin/sh
action=$1
if [ "x$STREAMS_TOPOLOGYX_PYTHON" == "x" ]
then
  STREAMS_TOPOLOGYX_PYTHON=python3
fi

if [ $action = "lib" ]
then
    ${STREAMS_TOPOLOGYX_PYTHON}-config --libs | sed -e 's/^-l//;s/ -l/ /g;s/ \+/\n/g'
elif [ $action = "libPath" ]
then
    echo `${STREAMS_TOPOLOGYX_PYTHON}-config --prefix`/lib
elif [ $action = "includePath" ]
then
    ${STREAMS_TOPOLOGYX_PYTHON}-config --includes
fi
