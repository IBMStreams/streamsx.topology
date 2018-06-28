/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.generator.spl;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.builder.BVirtualMarker;

import static com.ibm.streamsx.topology.builder.BVirtualMarker.END_PARALLEL;
import static com.ibm.streamsx.topology.generator.spl.GraphUtilities.isHashAdder;
import static com.ibm.streamsx.topology.generator.spl.GraphUtilities.kind;
import static com.ibm.streamsx.topology.generator.spl.GraphUtilities.operators;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;

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

    private void relocateHashAdders(){
        final Set<JsonObject> hashAdders = new HashSet<>();
        // First, find all HashAdders in the graph. The reason for not
        // moving HashAdders in this loop is to avoid modifying the graph
        // structure while traversing the graph.
        operators(graph, op -> {
            if (isHashAdder(op))
                hashAdders.add(op);
        });

        // Second, relocate HashAdders one by one.
        for(JsonObject hashAdder : hashAdders){
            relocateHashAdder(hashAdder);
        }
    }

    private void relocateHashAdder(JsonObject hashAdder){
        // Only optimize the case where $Unparallel$ is the HashAdder's only
        // parent and the HashAdder is $Unparallel$'s only child.
        // $Unparallel$ -> hashAdder -> $Parallel$ -> hashRemover
        Set<JsonObject> parents = GraphUtilities.getUpstream(hashAdder, graph);
        
        // check if hashAdder has only one parent
        if (parents.size() != 1) return;

        JsonObject parent = parents.iterator().next();
        // check if HashAdder's parent is $Unparallel$, and $Unparallel$
        // has only one child
        if (END_PARALLEL.isThis(kind(parent)) &&
                GraphUtilities.getDownstream(parent, graph).size() == 1) {
            // retrieve HashAdder's output port schema
            String schema = GraphUtilities.getOutputPortType(hashAdder, 0);
            // insert a copy of HashAdder to the front of Unparallel
            JsonObject hashAdderCopy = GraphUtilities.copyOperatorNewName(
                    hashAdder, jstring(hashAdder, "name"));
            GraphUtilities.removeOperator(hashAdder, graph);
            GraphUtilities.addBefore(parent, hashAdderCopy, graph);
            // set Unparallel's output port schema using HashAdder's schema
            GraphUtilities.setOutputPortType(parent, 0, schema);
        }
    }
}
