/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.builder;

public abstract class BOutput extends BJSONObject {
    
    public abstract String _type();

    public abstract void connectTo(BInputPort port);
}
