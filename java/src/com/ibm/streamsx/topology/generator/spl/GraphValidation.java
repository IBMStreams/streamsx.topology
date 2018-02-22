/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.generator.spl;

import static com.ibm.streamsx.topology.generator.spl.GraphUtilities.findOperatorByKind;
import static com.ibm.streamsx.topology.generator.spl.GraphUtilities.getDownstream;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.first;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;

import java.util.Set;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.builder.BVirtualMarker;

public class GraphValidation {
    
    void validateGraph(GCompositeDef gcomp){
        checkValidEndParallel(gcomp);
    }
    
    private void checkValidEndParallel(GCompositeDef gcomp){
        Set<JsonObject> endParallels = findOperatorByKind(BVirtualMarker.END_PARALLEL, gcomp);	

        for (JsonObject endParallel : endParallels) {
            // Setting up loop
            JsonObject endParallelParent = endParallel;
            do {
                Set<JsonObject> endParallelParents = gcomp.getUpstream(endParallelParent);
                if (endParallelParents.size() != 1) {
                    throw new IllegalStateException("Cannot union multiple streams before invoking endParallel()");
                }
                endParallelParent = first(endParallelParents);
            } while (jstring(endParallelParent, "kind").startsWith("$"));
            
            Set<JsonObject> endParallelParentChildren = gcomp.getDownstream(endParallelParent);
            if (endParallelParentChildren.size() != 1) {
                throw new IllegalStateException("Cannot fanout a stream before invoking endParallel()");
            }
        }
    }

}
