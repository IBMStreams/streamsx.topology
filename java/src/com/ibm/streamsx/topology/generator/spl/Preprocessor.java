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
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.CFG_HAS_ISOLATE;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.builder.BVirtualMarker;
import com.ibm.streamsx.topology.function.Consumer;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;

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
        Set<JsonObject> parallelOperators = findOperatorByKind(PARALLEL, graph);  
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
            Set<JsonObject> unionOps = GraphUtilities.findOperatorByKind(marker, graph);
            GraphUtilities.removeOperators(unionOps, graph);
        }
    }
    
    @SuppressWarnings("serial")
    private void relocateHashAdders(){
        final Set<JsonObject> hashAdders = new HashSet<>();
        // Firstly, find each hashAdder
        GraphUtilities.visitOnce(GraphUtilities.findStarts(graph), new HashSet<BVirtualMarker>(), graph, new Consumer<JsonObject>(){
            public void accept(JsonObject op) {
                if(jstring(op, "kind").equals("com.ibm.streamsx.topology.functional.java::HashAdder")){
                    hashAdders.add(op);
                }
            }
        });
        
        assertValidParallelUnions(hashAdders); 
        for(JsonObject hashAdder : hashAdders){
            relocateHashAdder(hashAdder);
        }
        
    }
    
    private void assertValidParallelUnions(Set<JsonObject> hashAdders) {   
        for(JsonObject hashAdder : hashAdders){
            Set<JsonObject> hashAdderParents = GraphUtilities.getUpstream(hashAdder, graph);
            Set<JsonObject> tmp = new HashSet<>();
            
            // Add all $Unparallel$ parents of hashAdder to list
            for(JsonObject hashAdderParent : hashAdderParents){
                if(jstring(hashAdderParent, "kind").equals(BVirtualMarker.END_PARALLEL.kind())){
                    tmp.add(hashAdderParent);
                }
            }
            hashAdderParents = tmp;
            
            // Assert that the downstream hashadders of each unparallel 
            // operator are all of the same routing type.
            for(JsonObject hashAdderParent : hashAdderParents){
                String lastRoutingType = null;
                Set<JsonObject> unparallelChildren = GraphUtilities.getDownstream(hashAdderParent, graph);
                for(JsonObject unparallelChild : unparallelChildren){
                    if(jstring(unparallelChild, "kind").equals("com.ibm.streamsx.topology.functional.java::HashAdder")
		       || jstring(unparallelChild, "kind").equals(BVirtualMarker.PARALLEL.kind())) {
                        if(lastRoutingType != null && !(jstring(unparallelChild, "routing")).equals(lastRoutingType)){
                            throw new IllegalStateException("A TStream from an endParallel invocation is being used to begin"
                                    + " two separate parallel regions that have two different kind of routing.");
                        }
                        lastRoutingType = jstring(unparallelChild, "routing");
                    }
                }          
            }           
        }
    }

    private void relocateHashAdder(JsonObject hashAdder){
        int numHashAdderCopies = 0;
        int numHashRemoverCopies = 0;
        String routing = GsonUtilities.jstring(hashAdder, "routing");

        // The hashremover object
        // hashAdder -> $Parallel$ -> $Isolate -> hashremover
        JsonObject hashRemover = GraphUtilities.getDownstream(hashAdder, graph).iterator().next();
        hashRemover = GraphUtilities.getDownstream(hashRemover, graph).iterator().next();
        hashRemover = GraphUtilities.getDownstream(hashRemover, graph).iterator().next();
        
        Set<JsonObject> children = GraphUtilities.getDownstream(hashAdder, graph);
        Set<JsonObject> parents = GraphUtilities.getUpstream(hashAdder, graph);
        
        JsonObject parallelStart = children.iterator().next();
        
        String inputPortName = GraphUtilities.getInputPortName(hashAdder, 0);
        String parallelInputPortName = GraphUtilities.getInputPortName(parallelStart, 0);
        
        List<JsonObject> unparallelParents = new ArrayList<>();
        List<JsonObject> nonUnparallelParents = new ArrayList<>();

        // Check whether a hashAdder has already been added before
        // the unparallel. If it has, remove it.
        for(JsonObject parent : parents){
            if(jstring(parent, "kind").equals(BVirtualMarker.END_PARALLEL.kind())){
                JsonObject upstreamOfUnparallelOp = GraphUtilities.getUpstream(parent, graph).iterator().next();
		// Need to jump over the auto-inserted $isolate operator
		upstreamOfUnparallelOp = GraphUtilities.getUpstream(upstreamOfUnparallelOp, graph).iterator().next();
                if(!(jstring(upstreamOfUnparallelOp, "kind")).equals("com.ibm.streamsx.topology.functional.java::HashAdder")){
                    if(!unparallelParents.contains(parent)){
                        unparallelParents.add(parent);
                    }
                }
            }
            else{
                if(!nonUnparallelParents.contains(parent)){
                    nonUnparallelParents.add(parent);
                }
            }
        }
        if(unparallelParents.size() == 0)
            return;
        
        for (JsonObject unparallelParent : unparallelParents) {
            // Add hashadder before unparallel
            JsonObject hashAdderCopy = GraphUtilities.copyOperatorNewName(hashAdder,
                    jstring(hashAdder, "name") + "_" + Integer.toString(numHashAdderCopies++));
            JsonObject isolateOp = GraphUtilities.getUpstream(unparallelParent, graph).iterator().next();
            GraphUtilities.addBefore(isolateOp, hashAdderCopy, graph);
        }
        
        for(JsonObject nonUnparallelParent : nonUnparallelParents){
            // Add hashadder after the nonUnparallelParent
            JsonObject hashAdderCopy = GraphUtilities.copyOperatorNewName(hashAdder, 
                    jstring(hashAdder, "name") + "_"+Integer.toString(numHashAdderCopies++));
            GraphUtilities.addBetween(nonUnparallelParent, hashAdder, hashAdderCopy);
            graph.get("operators").getAsJsonArray().add(hashAdderCopy);
        }
        
        // Get non-parallel, non hashremover children of unparallel regions and add 
        // a hashRemover between each.
        for(JsonObject unparallelParent : unparallelParents){
            Set<JsonObject> unparallelParentChildren = GraphUtilities.getDownstream(unparallelParent, graph);
            for(JsonObject unparallelParentChild : unparallelParentChildren){
                if(!jstring(unparallelParentChild, "kind").equals("com.ibm.streamsx.topology.functional.java::HashRemover")
		    && !jstring(unparallelParentChild,"kind").equals("com.ibm.streamsx.topology.functional.java::HashAdder")
                    && !jstring(unparallelParentChild,"kind").equals("$Parallel$")){
                    JsonObject hashRemoverCopy = GraphUtilities.copyOperatorNewName(hashRemover, 
                            jstring(hashRemover, "name") + "_"+Integer.toString(numHashRemoverCopies++));
                    GraphUtilities.addBetween(unparallelParent, unparallelParentChild, hashRemoverCopy);
                    graph.get("operators").getAsJsonArray().add(hashRemoverCopy);
                }
            }
        }
	GraphUtilities.removeOperator(hashAdder, graph);
    }
}
