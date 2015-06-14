/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.builder;

import com.ibm.streams.operator.StreamSchema;

public abstract class BOutput extends BJSONObject {

    public abstract StreamSchema schema();

    public abstract void connectTo(BInputPort port);

}
