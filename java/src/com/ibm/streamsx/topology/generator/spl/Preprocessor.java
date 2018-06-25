/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.generator.spl;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.builder.BVirtualMarker;
import com.ibm.streamsx.topology.function.Consumer;

/**
 * Preprocessor modifies the passed in JSON to perform
 * logical graph transformations.
 */
class Preprocessor {
    
    private final SPLGenerator generator;
    private final JsonObject graph;
    private final PEPlacement pePlacementPreprocess;
    
    Preprocessor(SPLGenerator generator, JsonObject graph) {
        this.generator = generator;
        this.graph = graph;
        pePlacementPreprocess = new PEPlacement(this.generator, graph);
    }
    
    Preprocessor preprocess() {
        
        GraphValidation graphValidationProcess = new GraphValidation();
        graphValidationProcess.validateGraph(graph);

        // The hash adder operators need to be relocated to enable directly
        // adjacent parallel regions
        relocateHashAdders();
        
        pePlacementPreprocess.tagIsolationRegions();
        pePlacementPreprocess.tagLowLatencyRegions();

        
        ThreadingModel.preProcessThreadedPorts(graph);
        
        removeRemainingVirtualMarkers();
        
        AutonomousRegions.preprocessAutonomousRegions(graph);
        
        pePlacementPreprocess.resolveColocationTags();

        // Optimize phase.
        new Optimizer(graph).optimize();
        
        return this;
    }
    
    private void removeRemainingVirtualMarkers(){
        for (BVirtualMarker marker : Arrays.asList(BVirtualMarker.UNION, BVirtualMarker.PENDING)) {
            List<JsonObject> unionOps = GraphUtilities.findOperatorByKind(marker, graph);
            GraphUtilities.removeOperators(unionOps, graph);
        }
    }

    public void compositeColocateIdUsage(List<JsonObject> composites) {
        if (composites.size() < 2)
            return;
        for (JsonObject composite : composites)
            pePlacementPreprocess.compositeColocateIdUse(composite);
    }

    @SuppressWarnings("serial")
    private void relocateHashAdders(){
        final Set<JsonObject> hashAdders = new HashSet<>();
        // Firstly, find each hashAdder
        GraphUtilities.visitOnce(GraphUtilities.findStarts(graph),
                new HashSet<BVirtualMarker>(),
                graph,
                new Consumer<JsonObject>(){
                    public void accept(JsonObject op) {
                        if(jstring(op, "kind").equals("com.ibm.streamsx.topology.functional.java::HashAdder")){
                            hashAdders.add(op);
                        }
                    }
        });

        for(JsonObject hashAdder : hashAdders){
            relocateHashAdder(hashAdder);
        }
    }

    private void relocateHashAdder(JsonObject hashAdder){
        // $Unparallel$ -> hashAdder -> $Parallel$ -> hashRemover
        Set<JsonObject> parents = GraphUtilities.getUpstream(hashAdder, graph);
        
        // hashAdder has multiple parents
        if (parents.size() != 1) return;

        JsonObject parent = parents.iterator().next();

        // hashAdder's parent has multiple children
        if (GraphUtilities.getDownstream(parent, graph).size() != 1)
            return;

        // Only optimize the case where $Unparallel has only one child, and
        // $parallel has only one parent
        if (jstring(parent, "kind").equals(BVirtualMarker.END_PARALLEL.kind())) {
            GraphUtilities.removeOperator(hashAdder, graph);
            GraphUtilities.addBefore(parent, hashAdder, graph);
        }
    }
}
