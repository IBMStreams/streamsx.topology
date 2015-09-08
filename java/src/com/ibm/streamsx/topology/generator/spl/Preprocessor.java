/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.generator.spl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import com.ibm.json.java.JSONObject;
import com.ibm.json.java.JSONArray;
import com.ibm.streamsx.topology.builder.BVirtualMarker;
import com.ibm.streamsx.topology.function.Consumer;

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

        GraphValidation graphValidationProcess = new GraphValidation();
        graphValidationProcess.validateGraph(graph);
        
        PEPlacement pePlacementPreprocess = new PEPlacement();

        // The hash adder operators need to be relocated to enable directly 
	// adjacent parallel regions
        relocateHashAdders();
        
        pePlacementPreprocess.tagIsolationRegions(graph);
        pePlacementPreprocess.tagLowLatencyRegions(graph);
        
        ThreadingModel.preProcessThreadedPorts(graph);
        
        // At this point, the $Union$ operators in the graph are just place holders.
        removeUnionOperators();
    }
       
    private void removeUnionOperators(){
        List<JSONObject> unionOps = GraphUtilities.findOperatorByKind(BVirtualMarker.UNION, graph);
        GraphUtilities.removeOperators(unionOps, graph);
    }
    
    @SuppressWarnings("serial")
    private void relocateHashAdders(){
        final List<JSONObject> hashAdders = new ArrayList<>();
        // Firstly, find each hashAdder
        GraphUtilities.visitOnce(GraphUtilities.findStarts(graph), new HashSet<BVirtualMarker>(), graph, new Consumer<JSONObject>(){
            public void accept(JSONObject op) {
                if(((String)op.get("kind")).equals("com.ibm.streamsx.topology.functional.java::HashAdder")){
                    hashAdders.add(op);
                }
            }
        });
        
        assertValidParallelUnions(hashAdders); 
        for(JSONObject hashAdder : hashAdders){
            relocateHashAdder(hashAdder);
        }
        
    }
    
    private void assertValidParallelUnions(List<JSONObject> hashAdders) {   
        for(JSONObject hashAdder : hashAdders){
            List<JSONObject> hashAdderParents = GraphUtilities.getUpstream(hashAdder, graph);
            List<JSONObject> tmp = new ArrayList<>();
            
            // Add all $Unparallel$ parents of hashAdder to list
            for(JSONObject hashAdderParent : hashAdderParents){
                if(((String)hashAdderParent.get("kind")).equals(BVirtualMarker.END_PARALLEL.kind())){
                    tmp.add(hashAdderParent);
                }
            }
            hashAdderParents = tmp;
            
            // Assert that the downstream hashadders of each unparallel 
            // operator are all of the same routing type.
            for(JSONObject hashAdderParent : hashAdderParents){
                String lastRoutingType = null;
                List<JSONObject> unparallelChildren = GraphUtilities.getDownstream(hashAdderParent, graph);
                for(JSONObject unparallelChild : unparallelChildren){
                    if(((String)unparallelChild.get("kind")).equals("com.ibm.streamsx.topology.functional.java::HashAdder")
		       || ((String)unparallelChild.get("kind")).equals(BVirtualMarker.PARALLEL.kind())){
                        if(lastRoutingType != null && !((String)unparallelChild.get("routing")).equals(lastRoutingType)){
                            throw new IllegalStateException("A TStream from an endParallel invocation is being used to begin"
                                    + " two separate parallel regions that have two different kind of routing.");
                        }
                        lastRoutingType = (String)unparallelChild.get("routing");
                    }
                }          
            }           
        }
    }

    private void relocateHashAdder(JSONObject hashAdder){
        int numHashAdderCopies = 0;
        int numHashRemoverCopies = 0;
        String routing = (String) hashAdder.get("routing");

        // The hashremover object
	// hashAdder -> $Parallel$ -> $Isolate -> hashremover
        JSONObject hashRemover = GraphUtilities.getDownstream(hashAdder, graph).get(0);
	hashRemover = GraphUtilities.getDownstream(hashRemover, graph).get(0);
	hashRemover = GraphUtilities.getDownstream(hashRemover, graph).get(0);
        
        List<JSONObject> children = GraphUtilities.getDownstream(hashAdder, graph);
        List<JSONObject> parents = GraphUtilities.getUpstream(hashAdder, graph);
        
        JSONObject parallelStart = children.get(0);
        
        String inputPortName = GraphUtilities.getInputPortName(hashAdder, 0);
        String parallelInputPortName = GraphUtilities.getInputPortName(parallelStart, 0);
        
        List<JSONObject> unparallelParents = new ArrayList<>();
        List<JSONObject> nonUnparallelParents = new ArrayList<>();

        // Check whether a hashAdder has already been added before
        // the unparallel. If it has, remove it.
        for(JSONObject parent : parents){
            if(((String)parent.get("kind")).equals(BVirtualMarker.END_PARALLEL.kind())){
                JSONObject upstreamOfUnparallelOp = GraphUtilities.getUpstream(parent, graph).get(0);
		// Need to jump over the auto-inserted $isolate operator
		upstreamOfUnparallelOp = GraphUtilities.getUpstream(upstreamOfUnparallelOp, graph).get(0);
                if(!((String)upstreamOfUnparallelOp.get("kind")).equals("com.ibm.streamsx.topology.functional.java::HashAdder")){
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
        
        for(JSONObject unparallelParent : unparallelParents){
            // Add hashadder before unparallel
            JSONObject hashAdderCopy = GraphUtilities.copyOperatorNewName(hashAdder, 
                    ((String)hashAdder.get("name")) +"_"+Integer.toString(numHashAdderCopies++));
	    JSONObject isolateOp = GraphUtilities.getUpstream(unparallelParent, graph).get(0);
            GraphUtilities.addBefore(isolateOp, hashAdderCopy, graph);
	    ((JSONArray)graph.get("operators")).add(hashAdderCopy);
        }
        
        for(JSONObject nonUnparallelParent : nonUnparallelParents){
            // Add hashadder after the nonUnparallelParent
            JSONObject hashAdderCopy = GraphUtilities.copyOperatorNewName(hashAdder, 
                    ((String)hashAdder.get("name")) + "_"+Integer.toString(numHashAdderCopies++));
            GraphUtilities.addBetween(nonUnparallelParent, hashAdder, hashAdderCopy);
            ((JSONArray)graph.get("operators")).add(hashAdderCopy);
        }
        
        // Get non-parallel, non hashremover children of unparallel regions and add 
        // a hashRemover between each.
        for(JSONObject unparallelParent : unparallelParents){
            List<JSONObject> unparallelParentChildren = GraphUtilities.getDownstream(unparallelParent, graph);
            for(JSONObject unparallelParentChild : unparallelParentChildren){
                if(!((String)unparallelParentChild.get("kind")).equals("com.ibm.streamsx.topology.functional.java::HashRemover")
		    && !((String)unparallelParentChild.get("kind")).equals("com.ibm.streamsx.topology.functional.java::HashAdder")
                    && !((String)unparallelParentChild.get("kind")).equals("$Parallel$")){
                    JSONObject hashRemoverCopy = GraphUtilities.copyOperatorNewName(hashRemover, 
                            ((String)hashRemover.get("name")) + "_"+Integer.toString(numHashRemoverCopies++));
                    GraphUtilities.addBetween(unparallelParent, unparallelParentChild, hashRemoverCopy);
		    ((JSONArray)graph.get("operators")).add(hashRemoverCopy);
                }
            }
        }
	GraphUtilities.removeOperator(hashAdder, graph);
    }
}
