package com.ibm.streamsx.topology.generator.spl;

import java.util.List;

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

        List<JSONObject> autonomousOperators = GraphUtilities.findOperatorByKind(
                BVirtualMarker.AUTONOMOUS, graph);

        for (JSONObject autonomous : autonomousOperators) {
        	List<JSONObject> startAutonomous = GraphUtilities.getDownstream(autonomous, graph);
        	for (JSONObject sa : startAutonomous) {
        		if (!sa.containsKey(AUTONOMOUS))
        		    sa.put(AUTONOMOUS, Boolean.TRUE);
        	}
        }
 
        GraphUtilities.removeOperators(autonomousOperators, graph);
    }
    
    /**
     * Add in the annotation.
     */
    static void autonomousAnnotation(JSONObject op, StringBuilder sb) {
    	Boolean set = (Boolean) op.get(AUTONOMOUS);
    	if (set != null && set)
    		sb.append("@autonomous\n");
    }
}
