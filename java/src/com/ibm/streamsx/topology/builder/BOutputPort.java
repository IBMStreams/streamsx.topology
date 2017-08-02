/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.builder;

import com.ibm.streams.flow.declare.OutputPortDeclaration;
import com.ibm.streams.operator.StreamSchema;

public class BOutputPort extends BOutput implements BPort {

    private final BOperatorInvocation op;
    private final OutputPortDeclaration port;

    BOutputPort(BOperatorInvocation op, int index, String name, StreamSchema schema) {
        this.op = op;
        addPortInfo(index, name, schema);
        this.port = op.op().addOutput(name, schema);
    }

    public BOperatorInvocation operator() {
        return op;
    }

    public GraphBuilder builder() {
        return op.builder();
    }

    private OutputPortDeclaration port() {
        return port;
    }

    @Override
    public StreamSchema schema() {
        return port.getStreamSchema();
    }

    @Override
    public void connectTo(BInputPort input) {
        connect(input);
        input.connect(this);
               
        input.port().connect(port());
        input.operator().copyRegions(operator());
    }
}
