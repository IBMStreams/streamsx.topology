/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.builder;

import java.util.concurrent.TimeUnit;

import com.google.gson.JsonObject;

/**
 * Input ports don't have a name in SPL but the code generation
 * keys ports by their name so we create a unique internal identifier
 * for the name.
 */
public class BInputPort extends BInput {
    
    public interface Window {
        String SLIDING = "SLIDING";
        
        String NONE_POLICY = "NONE";
        String TIME_POLICY = "TIME";
        String COUNT_POLICY = "COUNT";
    }

    private final BOperator op;

    BInputPort(BOperatorInvocation op, int index, String schema) {
        super(op.builder());
        this.op = op;
        
        addPortInfo(index, op.builder().uniqueId("$__spl_ip"), schema);
    }

    public BOperator operator() {
        return op;
    }

    public BInputPort window(String type,
            String evictPolicy, Object evictConfig, TimeUnit evictTimeUnit,
            String triggerPolicy, Object triggerConfig, TimeUnit triggerTimeUnit,
            boolean partitioned) {

        final JsonObject winJson = new JsonObject();
        winJson.addProperty("type", type);

        // Eviction
        switch (evictPolicy) {
        case Window.COUNT_POLICY:
        case Window.TIME_POLICY:
            break;
        default:
            throw new UnsupportedOperationException(evictPolicy);
        }
        winJson.addProperty("evictPolicy", evictPolicy);
        winJson.addProperty("evictConfig", (Number) evictConfig);
        if (evictPolicy.equals(Window.TIME_POLICY))
            winJson.addProperty("evictTimeUnit", evictTimeUnit.name());

        if (triggerPolicy != null && !triggerPolicy.equals(Window.NONE_POLICY)) {
            switch (triggerPolicy) {
            case Window.COUNT_POLICY:
            case Window.TIME_POLICY:
                break;
            default:
                throw new UnsupportedOperationException(triggerPolicy);
            }

            winJson.addProperty("triggerPolicy", triggerPolicy);
            winJson.addProperty("triggerConfig", (Number) triggerConfig);
            if (triggerTimeUnit != null)
                winJson.addProperty("triggerTimeUnit", triggerTimeUnit.name());
        }

        if (partitioned) {
            winJson.addProperty("partitioned", partitioned);
        }

        _json().add("window", winJson);

        return this;
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
