/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.generator.spl;

import java.util.ArrayList;
import java.util.List;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.json.java.OrderedJSONObject;
import com.ibm.streamsx.topology.function.Consumer;

class ThreadingModel {
    
    @SuppressWarnings("serial")
    static void preProcessThreadedPorts(final JSONObject graph){
        // Remove the threaded port configuration from the operator and its 
        // params if:
        // 1) The operator has a lowLatencyTag assigned
        // 2) The upstream operator has a different colocationTag as the 
        //    the operator.
        
        // Added threaded port configuration if the operator is non-functional
        // and it has a threaded port.
        
        ArrayList<JSONObject> starts = GraphUtilities.findStarts(graph);
        GraphUtilities.visitOnce(starts, null, graph, new Consumer<JSONObject>(){

            @Override
            public void accept(JSONObject op) {
                // These booleans will be used to determine whether to delete the
                // threaded port from the operator.         
                boolean regionTagExists = false;
                boolean differentColocationThanParent = false;
                boolean functional=false;
                
                JSONArray inputs = (JSONArray) op.get("inputs");
                
                // Currently, threadedPorts are only supported on operators
                // with one input port.
                if(inputs == null || inputs.size() != 1){
                    return;
                }
                
                JSONObject input = (JSONObject)inputs.get(0);
                JSONObject queue = (JSONObject) input.get("queue");
                // If the queue is null, simply return. Nothing to be done.
                if(queue == null){
                    return;
                }

                // If the operator is not functional, the we don't have to 
                // remove anything from the operator's params.
                functional = (boolean) queue.get("functional");

                // See if operator is in a lowLatency region
                String regionTag = (String) op.get("lowLatencyTag");
                if (regionTag != null && !regionTag.isEmpty()) {
                    regionTagExists = true;
                }               
                
                // See if operator has different colocation tag than any of 
                // its parents.
                String colocTag = null;
                JSONObject config = (JSONObject) op.get("config");
                colocTag = (String) config.get("colocationTag");

                List<JSONObject> parents = GraphUtilities.getUpstream(op, graph);
                for(JSONObject parent : parents){
                    JSONObject parentConfig = (JSONObject)parent.get("config");
                    String parentColocTag  = (String) parentConfig.get("colocationTag");
                    // Test whether colocation tags are different. If they are,
                    // don't insert a threaded port.
                    if(!colocTag.equals(parentColocTag)){
                        differentColocationThanParent = true;
                    }
                }
                
                // Remove the threaded port if necessary
                if(differentColocationThanParent || regionTagExists){
                    input.remove(queue);
                    if(functional){
                        JSONObject params = (JSONObject) op.get("parameters");
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
                    JSONObject newQueue = new OrderedJSONObject();
                    newQueue.put("queueSize", new Integer(100));
                    newQueue.put("inputPortName", input.get("name").toString());
                    newQueue.put("congestionPolicy", "Sys.Wait");
                    config.put("queue", newQueue);
                }          
           }

        });
    }
}
