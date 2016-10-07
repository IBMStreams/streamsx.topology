/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.generator.spl;

import java.util.List;
import java.util.Set;

import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.builder.BVirtualMarker;

public class GraphValidation {
    
    void validateGraph(JSONObject graph){
        checkValidEndParallel(graph);
    }
    
    private void checkValidEndParallel(JSONObject graph){
        Set<JSONObject> endParallels = GraphUtilities.findOperatorByKind(BVirtualMarker.END_PARALLEL, graph);	

        for(JSONObject endParallel : endParallels){
	    Set<JSONObject> endParallelParents = null;
	    // Setting up loop
	    JSONObject endParallelParent = endParallel;
            do{
		endParallelParents = GraphUtilities.getUpstream(endParallelParent, graph);
		if(endParallelParents.size() != 1){
		    throw new IllegalStateException("Cannot union multiple streams before invoking endParallel()");
		}
		endParallelParent = endParallelParents.toArray(new JSONObject[1])[0];
	    } while(((String)endParallelParent.get("kind")).startsWith("$"));
            Set<JSONObject> endParallelParentChildren = GraphUtilities.getDownstream(endParallelParent, graph);
            if(endParallelParentChildren.size() != 1){
                throw new IllegalStateException("Cannot fanout a stream before invoking endParallel()");
            }
        }
    }

}
