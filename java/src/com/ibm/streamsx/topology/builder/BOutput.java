/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.builder;

public abstract class BOutput extends BJSONObject implements BPort {
    
    public abstract String _type();

    public abstract void connectTo(BInputPort port);
    
    /**
     * Get the operator to which this output port is attached.
     * @since v1.9
     * @return The operator to which this output port is attached.
     */
    public abstract BOperator operator();
}
