/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.builder;

import java.util.concurrent.TimeUnit;

import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.window.StreamWindow;

public class BInputPort extends BInput implements BPort {

    private final BOperator op;

    BInputPort(BOperatorInvocation op, int index, String name, StreamSchema schema) {
        super(op.builder());
        this.op = op;
        
        addPortInfo(index, name, schema);
    }

    public BOperator operator() {
        return op;
    }

    public BInputPort window(StreamWindow.Type type,
            StreamWindow.Policy evictPolicy, Object evictConfig, TimeUnit evictTimeUnit,
            StreamWindow.Policy triggerPolicy, Object triggerConfig, TimeUnit triggerTimeUnit,
            boolean partitioned) {

        final JSONObject winJson = new JSONObject();
        winJson.put("type", type.name());

        // Eviction
        switch (evictPolicy) {
        case COUNT:
        case TIME:
            break;
        default:
            throw new UnsupportedOperationException(evictPolicy.name());
        }
        winJson.put("evictPolicy", evictPolicy.name());
        winJson.put("evictConfig", evictConfig);
        if (evictPolicy == StreamWindow.Policy.TIME)
            winJson.put("evictTimeUnit", evictTimeUnit.name());

        if (triggerPolicy != null && triggerPolicy != StreamWindow.Policy.NONE) {
            switch (triggerPolicy) {
            case COUNT:
            case TIME:
                break;
            default:
                throw new UnsupportedOperationException(evictPolicy.name());
            }

            winJson.put("triggerPolicy", triggerPolicy.name());
            winJson.put("triggerConfig", triggerConfig);
            if (triggerTimeUnit != null)
                winJson.put("triggerTimeUnit", triggerTimeUnit.name());
        }

        if (partitioned) {
            winJson.put("partitioned", partitioned);
        }

        json().put("window", winJson);

        return this;
    }
    
    public StreamSchema schema() {
        return __schema();
    }
    
    /**
     * Add a declaration of a default queue to this input port.
     * @param functional True if this is for a functional operator.
     */
    public void addQueue(boolean functional) {
        /* TODO - investigate
         * Disable queuing for now, seeing a hang when
         * using Java 8 and Streams 4.0.1 plus need
         * to investigate performance impact more.
         * 

        JSONObject queue = new JSONObject();
        queue.put("functional", functional);
        json().put("queue", queue);
        if (functional) {
            JSONObject params = (JSONObject) op.json().get("parameters");
            if (params == null) {
                params = new JSONObject();
                op.json().put("parameters", params);             
            }
            JSONObject value = new JSONObject();
            value.put("value", 100);
            params.put("queueSize", value);
        }
        */
    }
}
