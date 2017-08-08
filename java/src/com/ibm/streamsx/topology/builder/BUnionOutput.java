/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.builder;

import java.util.Set;

public class BUnionOutput extends BOutput {

    private final BOperator op;
    private final Set<BOutput> outputs;
    private final String schema;

    BUnionOutput(BOperator op, Set<BOutput> outputs) {
        this.op = op;
        this.outputs = outputs;
        schema = outputs.iterator().next()._type();
    }

    public BOperator operator() {
        return op;
    }

    @Override
    public String _type() {
        return schema;
    }

    @Override
    public void connectTo(BInputPort input) {
        for (BOutput output : outputs)
            output.connectTo(input);
    }
}
