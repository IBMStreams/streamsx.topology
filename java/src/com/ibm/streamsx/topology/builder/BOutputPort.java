/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.builder;

import com.ibm.streams.operator.StreamSchema;

public class BOutputPort extends BOutput implements BPort {

    private final BOperatorInvocation op;

    BOutputPort(BOperatorInvocation op, int index, String name, StreamSchema schema) {
        this.op = op;
        addPortInfo(index, name, schema);
    }

    public BOperatorInvocation operator() {
        return op;
    }

    public GraphBuilder builder() {
        return op.builder();
    }

    @Override
    public void connectTo(BInputPort input) {
        connect(input);
        input.connect(this);
               
        input.operator().copyRegions(operator());
    }
    @Override
    public StreamSchema schema() {
        return __schema();
    }
}
