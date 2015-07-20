/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.generator.spl;

import java.util.List;

import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.builder.GraphBuilder;

/**
 * Preprocessor modifies the passed in JSON to perform
 * logical graph transformations.
 */
class Preprocessor {
    
    private final JSONObject graph;
    
    Preprocessor(JSONObject graph) {
        this.graph = graph;
    }
    
    void preprocess() {
        
        PEPlacement pePlacementPreprocess = new PEPlacement();

        pePlacementPreprocess.tagIsolationRegions(graph);
        pePlacementPreprocess.tagLowLatencyRegions(graph);
        
        ThreadingModel.preProcessThreadedPorts(graph);
        
        // At this point, the $Union$ operators in the graph are just place holders.
        removeUnionOperators();
    }
       
    private void removeUnionOperators(){
        List<JSONObject> unionOps = GraphUtilities.findOperatorByKind(GraphBuilder.UNION, graph);
        GraphUtilities.removeOperators(unionOps, graph);
    }
}
