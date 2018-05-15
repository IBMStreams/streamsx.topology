/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.generator.spl;

import static com.ibm.streamsx.topology.builder.BVirtualMarker.END_PARALLEL;
import static com.ibm.streamsx.topology.builder.BVirtualMarker.ISOLATE;
import static com.ibm.streamsx.topology.builder.BVirtualMarker.PARALLEL;
import static com.ibm.streamsx.topology.generator.spl.GraphUtilities.addBefore;
import static com.ibm.streamsx.topology.generator.spl.GraphUtilities.findOperatorByKind;

import java.util.Arrays;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.builder.BVirtualMarker;

/**
 * Preprocessor modifies the passed in JSON to perform
 * logical graph transformations.
 */
class Preprocessor {
    
    private final SPLGenerator generator;
    private final JsonObject graph;
    
    Preprocessor(SPLGenerator generator, JsonObject graph) {
        this.generator = generator;
        this.graph = graph;
    }
    
    void preprocess() {
        
        GraphValidation graphValidationProcess = new GraphValidation();
        graphValidationProcess.validateGraph(graph);
        
        isolateParalleRegions();
        
        PEPlacement pePlacementPreprocess = new PEPlacement(generator, graph);

        // The hash adder operators need to be relocated to enable directly 
	// adjacent parallel regions
        // TODO: renable adjacent parallel regions optimization
        //relocateHashAdders();
        
        pePlacementPreprocess.tagIsolationRegions();
        pePlacementPreprocess.tagLowLatencyRegions();
        
        
        
        ThreadingModel.preProcessThreadedPorts(graph);
        
        removeRemainingVirtualMarkers();
        
        AutonomousRegions.preprocessAutonomousRegions(graph);
        
        pePlacementPreprocess.resolveColocationTags();

        // Optimize phase.
        new Optimizer(graph).optimize();       
    }
       
    /**
     * Isolate parallel regions to ensure that
     * we get parallelism through multiple PEs
     * (with the ability to have those PEs be distributed
     * across multiple hosts).
     * 
     * For 4.2 and later we achieve this using deploymentConfig
     * unless there are isolated regions.
     * 
     * Pre-4.2 we insert isolates prior to region and after the region.
     */
    private void isolateParalleRegions() {
        boolean needExplicitIsolates = !generator.versionAtLeast(4, 2);
        
        // TODO 4.2 checking
        
        if (!needExplicitIsolates)
            return;
        
        // Add isolate before the parallel and end parallel markers
        List<JsonObject> parallelOperators = findOperatorByKind(PARALLEL, graph);  
        parallelOperators.addAll(findOperatorByKind(END_PARALLEL, graph));
        for (JsonObject po : parallelOperators) {
            String schema = po.get("inputs").getAsJsonArray().get(0).getAsJsonObject().get("type").getAsString();
                        
            addBefore(po, newMarker(schema, ISOLATE), graph);         
        }       
    }
    
    
    private int ppMarkerCount;
    /**
     * Create a new marker operator that can be inserted into
     * the graph using addBefore.
     */
    private JsonObject newMarker(String schema, BVirtualMarker marker) {
        JsonObject op = new JsonObject();
        op.addProperty("marker", true);
        op.addProperty("kind", marker.kind());
        String name = "$$PreprocessorMarker_" + ppMarkerCount++;
        op.addProperty("name", name);
        
        {
            JsonArray inputs = new JsonArray();
            op.add("inputs", inputs);
            JsonObject input = new JsonObject();
            inputs.add(input);            
            input.addProperty("index", 0);
            input.addProperty("name", name + "_IN");
            input.addProperty("type", schema);
            input.add("connections", new JsonArray());
            
        }
        {
            JsonArray outputs = new JsonArray();
            op.add("outputs", outputs);
            JsonObject output = new JsonObject();
            outputs.add(output);
            output.addProperty("index", 0);
            output.addProperty("type", schema);
            output.add("connections", new JsonArray());
            output.addProperty("name", name + "_IN");
        }
        return op;
    }
    
    private void removeRemainingVirtualMarkers(){
        for (BVirtualMarker marker : Arrays.asList(BVirtualMarker.UNION, BVirtualMarker.PENDING)) {
            List<JsonObject> unionOps = GraphUtilities.findOperatorByKind(marker, graph);
            GraphUtilities.removeOperators(unionOps, graph);
        }
    }
}
