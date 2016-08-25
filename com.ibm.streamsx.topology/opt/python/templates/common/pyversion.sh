#!/bin/sh
# begin_generated_IBM_copyright_prolog                             
#                                                                  
# This is an automatically generated copyright prolog.             
# After initializing,  DO NOT MODIFY OR MOVE                       
# **************************************************************** 
# Licensed Materials - Property of IBM                             
# 5724-Y95                                                         
# (C) Copyright IBM Corp.  2016, 2016    All Rights Reserved.      
# US Government Users Restricted Rights - Use, duplication or      
# disclosure restricted by GSA ADP Schedule Contract with          
# IBM Corp.                                                        
#                                                                  
# end_generated_IBM_copyright_prolog                               
action=$1
if [ $action = "lib" ]
then
    python3-config --libs | sed -e 's/^-l//;s/ -l/ /g;s/ \+/\n/g'
elif [ $action = "libPath" ]
then
    echo `python3-config --prefix`/lib
elif [ $action = "includePath" ]
then
    python3-config --includes
fi
