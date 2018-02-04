package com.ibm.streamsx.topology.generator.spl;

import java.util.Set;

import com.google.gson.JsonObject;

/**
 * The GComposite interface contains methods for accessing and modifying
 * elements of a JsonObject graph, and encapsulate other objects 
 * with the graphs such as search indexes..
 *
 */
public interface GCompositeDef {
    /**
     * Return the JsonObject graph of operators contained in the composite. 
     * @return The JsonObject graph.
     */
    public JsonObject getGraph();
    
    /**
     * Returns all operators that have input ports connected to the output 
     * port of the supplied operator.
     * @param op
     * @return A list of downstream operators.
     */
    public Set<JsonObject> getDownstream(JsonObject op);
    
    /**
     * Returns all operators that have output ports connected to the input 
     * port of the supplied operator.
     * @param op
     * @return A list of upstream operators.
     */
    public Set<JsonObject> getUpstream(JsonObject op);
}
