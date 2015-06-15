/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.builder;

import java.util.Set;

import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.StreamSchema;

public class BUnionOutput extends BOutput {

    private final BOperator op;
    private final Set<BOutput> outputs;
    private final StreamSchema schema;

    BUnionOutput(BOperator op, Set<BOutput> outputs) {
        this.op = op;
        this.outputs = outputs;
        schema = outputs.iterator().next().schema();
    }

    public BOperator operator() {
        return op;
    }

    public GraphBuilder composite() {
        return op.builder();
    }

    @Override
    public JSONObject complete() {

        return json();
    }

    @Override
    public StreamSchema schema() {
        return schema;
    }

    @Override
    public void connectTo(BInputPort input) {
        for (BOutput output : outputs)
            output.connectTo(input);
    }
}
