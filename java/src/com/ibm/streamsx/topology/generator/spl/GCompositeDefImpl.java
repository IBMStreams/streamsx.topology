package com.ibm.streamsx.topology.generator.spl;

import java.util.Set;

import com.google.gson.JsonObject;

public class GCompositeDefImpl implements GCompositeDef {
    JsonObject graph;
    
    public GCompositeDefImpl(JsonObject graph) {
        this.graph = graph;
        
        //TODO build indexes to speed up upstream/downstream search.
    }

    @Override
    public JsonObject getGraph() {
        return this.graph;
    }

    @Override
    public Set<JsonObject> getUpstream(JsonObject op) {
        return GraphUtilities.getUpstream(op, graph);
    }
    
    @Override
    public Set<JsonObject> getDownstream(JsonObject op) {
        return GraphUtilities.getDownstream(op, graph);
    }
}
