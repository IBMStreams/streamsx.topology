/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.generator.spl;

import static com.ibm.streamsx.topology.generator.spl.GraphUtilities.findOperatorsByKinds;
import static com.ibm.streamsx.topology.generator.spl.GraphUtilities.getDownstream;
import static com.ibm.streamsx.topology.generator.spl.GraphUtilities.kind;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.array;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;

import java.util.HashSet;
import java.util.Set;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

/**
 * Optimize takes the preprocessed graph and adds any optimization.
 */
class Optimizer {

    private final JsonObject graph;

    Optimizer(JsonObject graph) {
        this.graph = graph;
    }

    void optimize() {
        pyPassByRef();
    }

    private static final String PY_OP_NS = "com.ibm.streamsx.topology.functional.python";
    private static final Set<String> PY_FUNC_OPS = new HashSet<>();

    static {
        for (String kind : new String[] { "Source", "Filter", "Map", "FlatMap", "ForEach", "Aggregate"}) {
            PY_FUNC_OPS.add(PY_OP_NS + "::" + kind);
            PY_FUNC_OPS.add(PY_OP_NS + "2::" + kind);
        }
    }

    /**
     * Setup Python operators to allow pass by reference.
     * 
     * Finds Python functional operators and sets the outputConnections
     * parameter representing the number of connections.
     * 
     * If pass by reference cannot be used outputConnections will not be set.
     * 
     * Does not modify the structure of the graph.
     * Assumes the graph's structure will not be subsequently modified.
     */
    private final void pyPassByRef() {
        Set<JsonObject> pyops = findOperatorsByKinds(graph, PY_FUNC_OPS);

        if (pyops.isEmpty())
            return;

        for (JsonObject pyop : pyops) {
            JsonArray outputs = array(pyop, "outputs");
            if (outputs == null || outputs.size() == 0)
                continue;

            // Currently only supporting a single output port
            // though mostly coded to support N.
            assert outputs.size() == 1;

            int[] connCounts = new int[outputs.size()];

            for (int port = 0; port < connCounts.length; port++) {
                connCounts[port] = -1;

                JsonObject output = outputs.get(port).getAsJsonObject();

                // Can't use the schema objects as we need to not depend on IBM Streams
                // classes.
                if (!"tuple<blob __spl_po>".equals(jstring(output, "type")))
                    continue;

                JsonArray conns = array(output, "connections");
                if (conns == null || conns.size() == 0) {
                    connCounts[port] = 0;
                    continue;
                }
                
                boolean canPassByRef = true;
                // TOOD - downstream for a specific port
                Set<JsonObject> connected = getDownstream(pyop, graph);
                for (JsonObject connectedOp : connected) {
                    if (!PY_FUNC_OPS.contains(kind(connectedOp))) {
                        canPassByRef = false;
                        break;
                    }
                        
                        // TEMP
                        // Currently only Map and ForEach completly handle
                        // by reference.
                        if (!kind(connectedOp).endsWith("::Map") && !kind(connectedOp).endsWith("::ForEach") && !kind(connectedOp).endsWith("::FlatMap")
			    && !kind(connectedOp).endsWith("::Aggregate")) {
                        canPassByRef = false;
                        break;
                        }
                }

                if (canPassByRef)
                    connCounts[port] = conns.size();
            }

            boolean paramNeeded = false;
            for (int oc: connCounts) {
                if (oc != -1) {
                    paramNeeded = true;
                    break;
                }
            }

            if (paramNeeded) {
                JsonObject value = new JsonObject();
                if (connCounts.length == 1)
                     value.addProperty("value", connCounts[0]);
                else {
                    JsonArray ocs = new JsonArray();
                    for (int oc : connCounts)
                        ocs.add(new JsonPrimitive(oc));
                    value.add("value", ocs);
                }
                GraphUtilities.addOpParameter(pyop, "outputConnections", value);
            }
        }
    }
}
