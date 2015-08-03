/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.builder;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.streams.flow.declare.OutputPortDeclaration;
import com.ibm.streams.flow.declare.StreamConnection;
import com.ibm.streams.operator.StreamSchema;

public class BOutputPort extends BOutput {

    private final BOperatorInvocation op;
    private final OutputPortDeclaration port;

    BOutputPort(BOperatorInvocation op, OutputPortDeclaration port) {
        this.op = op;
        this.port = port;
    }

    public BOperatorInvocation operator() {
        return op;
    }

    public GraphBuilder builder() {
        return op.builder();
    }

    @Override
    public JSONObject complete() {

        final JSONObject json = json();

        BUtils.addPortInfo(json, port);

        JSONArray conns = new JSONArray();
        for (StreamConnection c : port().getConnections()) {
            conns.add(c.getInput().getName());
        }
        json.put("connections", conns);

        return json;
    }

    public OutputPortDeclaration port() {
        return port;
    }

    @Override
    public StreamSchema schema() {
        return port.getStreamSchema();
    }

    @Override
    public void connectTo(BInputPort input) {
        input.port().connect(port());
        input.operator().copyRegions(operator());
    }
}
