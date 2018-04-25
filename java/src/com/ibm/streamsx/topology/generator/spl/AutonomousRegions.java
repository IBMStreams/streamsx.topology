package com.ibm.streamsx.topology.generator.spl;

import static com.ibm.streamsx.topology.generator.spl.GraphUtilities.getDownstream;

import java.util.List;
import java.util.Set;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.builder.BVirtualMarker;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;

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
    static void preprocessAutonomousRegions(JsonObject graph) {

        List<JsonObject> autonomousOperators = GraphUtilities.findOperatorByKind(
                BVirtualMarker.AUTONOMOUS, graph);

        for (JsonObject autonomous : autonomousOperators) {
        	for (JsonObject sa : getDownstream(autonomous, graph)) {
        		if (!sa.has(AUTONOMOUS))
        		    sa.addProperty(AUTONOMOUS, Boolean.TRUE);
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
