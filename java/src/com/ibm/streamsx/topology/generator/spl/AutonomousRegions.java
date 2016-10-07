package com.ibm.streamsx.topology.generator.spl;

import static com.ibm.streamsx.topology.generator.spl.GraphUtilities.getDownstream;

import java.util.List;
import java.util.Set;

import com.google.gson.JsonObject;
import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.builder.BVirtualMarker;

/**
 * Processing related to autonomous regions.
 * 
 * After pre-processing the JSON for the operator
 * can contain the key {@code autonomous} set to true
 * if the operator needs to be annotated.
 *
 */
class AutonomousRegions {
	
	static final String AUTONOMOUS = "autonomous";
	
	/**
	 * Preprocess autonomous virtual markers to mark
	 * downstream operators as autonomous.  
	 */
    static void preprocessAutonomousRegions(JSONObject graph) {

        Set<JSONObject> autonomousOperators = GraphUtilities.findOperatorByKind(
                BVirtualMarker.AUTONOMOUS, graph);

        for (JSONObject autonomous : autonomousOperators) {
        	for (JSONObject sa : getDownstream(autonomous, graph)) {
        		if (!sa.containsKey(AUTONOMOUS))
        		    sa.put(AUTONOMOUS, Boolean.TRUE);
        	}
        }
 
        GraphUtilities.removeOperators(autonomousOperators, graph);
    }
    
    /**
     * Add in the annotation.
     */
    static void autonomousAnnotation(JsonObject op, StringBuilder sb) {
    	boolean set = GsonUtilities.jboolean(op, AUTONOMOUS);
    	if (set)
    		sb.append("@autonomous\n");
    }
}
