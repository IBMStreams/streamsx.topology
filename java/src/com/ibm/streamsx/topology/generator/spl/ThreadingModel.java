/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.generator.spl;

import static com.ibm.streamsx.topology.generator.spl.GraphUtilities.getUpstream;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.array;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jboolean;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jobject;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.nestedObject;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.objectCreate;

import java.util.Set;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.function.Consumer;
import com.ibm.streamsx.topology.generator.operator.OpProperties;

class ThreadingModel {
    
    @SuppressWarnings("serial")
    static void preProcessThreadedPorts(final JsonObject graph){
        // Remove the threaded port configuration from the operator and its 
        // params if:
        // 1) The operator has a lowLatencyTag assigned
        // 2) The upstream operator has a different colocationTag as the 
        //    the operator.
        
        // Added threaded port configuration if the operator is non-functional
        // and it has a threaded port.
        
        Set<JsonObject> starts = GraphUtilities.findStarts(graph);
        GraphUtilities.visitOnce(starts, null, graph, new Consumer<JsonObject>(){

            @Override
            public void accept(JsonObject op) {
                // These booleans will be used to determine whether to delete the
                // threaded port from the operator.         
                boolean regionTagExists = false;
                boolean differentColocationThanParent = false;
                boolean functional=false;
                
                JsonArray inputs = array(op, "inputs");
                
                // Currently, threadedPorts are only supported on operators
                // with one input port.
                if(inputs == null || inputs.size() != 1){
                    return;
                }
                
                JsonObject input = inputs.get(0).getAsJsonObject();
                JsonObject queue = jobject(input, "queue");
                // If the queue is null, simply return. Nothing to be done.
                if(queue == null){
                    return;
                }

                // If the operator is not functional, the we don't have to 
                // remove anything from the operator's params.
                functional = jboolean(queue, "functional");

                JsonObject placement = jobject(op, OpProperties.PLACEMENT);
                
                // See if operator is in a lowLatency region
                String regionTag = null;
                if (placement != null) {
                    regionTag = jstring(placement, OpProperties.PLACEMENT_LOW_LATENCY_REGION_ID);
                }
                if (regionTag != null && !regionTag.isEmpty()) {
                    regionTagExists = true;
                }               
                
                // See if operator has different colocation tag than any of 
                // its parents.

                String colocTag = null;
                if (placement != null) {
                    colocTag = jstring(placement, OpProperties.PLACEMENT_ISOLATE_REGION_ID);
                }

                for(JsonObject parent : getUpstream(op, graph)){
                    JsonObject parentPlacement = nestedObject(parent, OpProperties.CONFIG, OpProperties.PLACEMENT);
                    String parentColocTag = null;
                    if (parentPlacement != null)
                        parentColocTag = jstring(parentPlacement, OpProperties.PLACEMENT_ISOLATE_REGION_ID);
                    // Test whether colocation tags are different. If they are,
                    // don't insert a threaded port.
                    if(!colocTag.equals(parentColocTag)){
                        differentColocationThanParent = true;
                    }
                }
                
                // Remove the threaded port if necessary
                if(differentColocationThanParent || regionTagExists){
                    input.remove("queue");
                    if(functional){
                        JsonObject params = jobject(op, "parameters");
                        params.remove("queueSize");
                    }
                }
                
                if(functional && 
                        !(differentColocationThanParent || regionTagExists)){
                    return;
                }
                
                // Add to SPL operator config if necessary
                if(!functional && 
                        !(differentColocationThanParent || regionTagExists)){
                    JsonObject newQueue = objectCreate(op, OpProperties.CONFIG, "queue");
                    newQueue.addProperty("queueSize", new Integer(100));
                    newQueue.addProperty("inputPortName", input.get("name").getAsString());
                    newQueue.addProperty("congestionPolicy", "Sys.Wait");
                }          
           }

        });
    }
}
