/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.builder;

import static com.ibm.streamsx.topology.generator.operator.OpProperties.LANGUAGE;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.MODEL;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.addToObject;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;

import java.util.HashSet;
import java.util.Set;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.builder.JOperator.JOperatorConfig;
import com.ibm.streamsx.topology.generator.operator.OpProperties;
import com.ibm.streamsx.topology.generator.spl.GraphUtilities;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;

/**
 * JSON representation.
 * 
 * Operator:
 * 
 * params: Object of Parameters, keyed by parameter name. outputs: Array of
 * output ports (see BStream) inputs: Array in input ports type: connections:
 * array of connection names as strings
 * 
 * Parameter value: Value of parameter.
 */

public class BOperator extends BJSONObject {

    private final GraphBuilder bt;

    private Set<String> regions;

    public BOperator(GraphBuilder bt) {
        this.bt = bt;
    }

    public GraphBuilder builder() {
        return bt;
    }
    
    public final String kind() {
        return GraphUtilities.kind(_json());
    }
    public final String model() {
        return jstring(_json(), MODEL);
    }
    public final String language() {
        return jstring(_json(), LANGUAGE);
    }
    public boolean isVirtual() {
        return BVirtualMarker.isVirtualMarker(kind());
    }

    public boolean addRegion(String name) {
        if (regions == null)
            regions = new HashSet<>();

        return regions.add(name);
    }

    public boolean copyRegions(BOperator otherOp) {
        if (otherOp.regions == null)
            return false;

        if (regions == null)
            regions = new HashSet<>();

        return regions.addAll(otherOp.regions);
    }
    
    /**
     * Add a configuration value to this operator.
     * The value must be a value acceptable to JSON
     * and is put into the "config" object of this operator.
     * @param key
     * @param value
     */
    public void addConfig(String key, Object value) {       
        JOperatorConfig.addItem(_json(), key, value);    
    }
    
    /**
     * Get an existing configuration value.
     * @param key Key of the value.
     * @return The configured value, or null if it has not been set.
     */
    public JsonElement getConfigItem(String key) {
        return JOperatorConfig.getItem(_json(), key);
    }

    @Override
    public JsonObject _complete() {
        JsonObject json = super._complete();
        if (regions != null && !regions.isEmpty()) {
            addToObject(json, "regions", regions);
        }
        return json;
    }
}
