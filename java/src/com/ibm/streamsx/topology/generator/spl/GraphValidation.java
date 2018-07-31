/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.generator.spl;

import static com.ibm.streamsx.topology.generator.spl.GraphUtilities.findOperatorByKind;
import static com.ibm.streamsx.topology.generator.spl.GraphUtilities.getDownstream;
import static com.ibm.streamsx.topology.generator.spl.GraphUtilities.getUpstream;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.first;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;

import java.util.List;
import java.util.Set;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.builder.BVirtualMarker;
import com.ibm.streamsx.topology.internal.messages.Messages;

public class GraphValidation {
    
    void validateGraph(JsonObject graph){
        checkValidEndParallel(graph);
    }
    
    private void checkValidEndParallel(JsonObject graph){
        List<JsonObject> endParallels = findOperatorByKind(BVirtualMarker.END_PARALLEL, graph);	

        for (JsonObject endParallel : endParallels) {
            // Setting up loop
            JsonObject endParallelParent = endParallel;
            do {
                Set<JsonObject> endParallelParents = getUpstream(endParallelParent, graph);
                if (endParallelParents.size() != 1) {
                    throw new IllegalStateException(Messages.getString("GENERATOR_CANNOT_UNION"));
                }
                endParallelParent = first(endParallelParents);
            } while (jstring(endParallelParent, "kind").startsWith("$"));
            
            //Set<JsonObject> endParallelParentChildren = getDownstream(endParallelParent, graph);
            //if (endParallelParentChildren.size() != 1) {
            //    throw new IllegalStateException(Messages.getString("GENERATOR_CANNOT_FANOUT"));
            //}
        }
    }

}
