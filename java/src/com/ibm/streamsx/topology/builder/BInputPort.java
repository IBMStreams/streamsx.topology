/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.builder;

import java.util.concurrent.TimeUnit;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.streams.flow.declare.InputPortDeclaration;
import com.ibm.streams.flow.declare.StreamConnection;
import com.ibm.streams.operator.window.StreamWindow;

public class BInputPort extends BInput {

    private final BOperator op;
    private final InputPortDeclaration port;

    BInputPort(BOperator op, InputPortDeclaration port) {
        super(op.builder());
        this.op = op;
        this.port = port;
    }

    public BOperator operator() {
        return op;
    }

    /**
     * Add this port information and its connections to output ports, by name.
     */
    @Override
    public JSONObject complete() {

        final JSONObject json = json();

        BUtils.addPortInfo(json, port);

        JSONArray conns = new JSONArray();
        for (StreamConnection c : port().getConnections()) {
            conns.add(c.getOutput().getName());
        }
        json.put("connections", conns);

        return json;
    }

    public InputPortDeclaration port() {
        return port;
    }

    public BInputPort window(StreamWindow.Type type,
            StreamWindow.Policy evictPolicy, Object evictConfig,
            StreamWindow.Policy triggerPolicy, Object triggerConfig,
            boolean partitioned) {

        switch (type) {
        case NOT_WINDOWED:
            return this;
        case SLIDING:
            port().sliding();
            break;
        case TUMBLING:
            port().tumbling();
            break;
        }

        final JSONObject winJson = new JSONObject();
        winJson.put("type", type.name());

        // Eviction
        switch (evictPolicy) {
        case COUNT:
            port().evictCount(((Number) evictConfig).intValue());
            break;
        case TIME:
            port().evictTime((Long) evictConfig, TimeUnit.MILLISECONDS);
            break;
        default:
            ;
            throw new UnsupportedOperationException(evictPolicy.name());
        }
        winJson.put("evictPolicy", evictPolicy.name());
        winJson.put("evictConfig", evictConfig);

        if (triggerPolicy != null && triggerPolicy != StreamWindow.Policy.NONE) {
            switch (triggerPolicy) {
            case COUNT:
                port().triggerCount(((Number) triggerConfig).intValue());
                break;
            case TIME:
                port().triggerTime((Long) evictConfig, TimeUnit.MILLISECONDS);
                break;
            default:
                ;
                throw new UnsupportedOperationException(evictPolicy.name());
            }

            winJson.put("triggerPolicy", triggerPolicy.name());
            winJson.put("triggerConfig", triggerConfig);
        }

        if (partitioned) {
            port().partitioned();
            winJson.put("partitioned", partitioned);
        }

        json().put("window", winJson);

        return this;
    }

}
