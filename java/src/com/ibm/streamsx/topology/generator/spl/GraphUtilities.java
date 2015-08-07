package com.ibm.streamsx.topology.generator.spl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.function.Consumer;

class GraphUtilities {
    static ArrayList<JSONObject> findStarts(JSONObject graph) {
        ArrayList<JSONObject> starts = new ArrayList<JSONObject>();
        JSONArray ops = (JSONArray) graph.get("operators");
        for (int k = 0; k < ops.size(); k++) {
            JSONObject op = (JSONObject) (ops.get(k));
            JSONArray inputs = (JSONArray) op.get("inputs");
            if (inputs == null || inputs.size() == 0) {
                if (((JSONObject) ops.get(k)).get("name") != null
                        && !((String) ((JSONObject) ops.get(k)).get("name"))
                                .startsWith("$")) {
                    starts.add((JSONObject) ops.get(k));
                }
            }
        }
        return starts;
    }

    static ArrayList<JSONObject> findOperatorByKind(String opKind,
            JSONObject graph) {
        ArrayList<JSONObject> kindOperators = new ArrayList<JSONObject>();
        JSONArray ops = (JSONArray) graph.get("operators");
        for (int k = 0; k < ops.size(); k++) {
            JSONObject op = (JSONObject) (ops.get(k));
            String kind = (String) op.get("kind");
            if (kind != null && kind.equals(opKind)) {
                kindOperators.add(op);
            }
        }
        return kindOperators;
    }

    /**
     * Get all operators immediately downstream of the {@code visitOp}
     * operator. No exceptions are made for marker operators. 
     * <br><br>
     * Specifically, searches the graph, and returns a list of operators which 
     * have an input port connected to any output port of {@code visitOp}.
     * @param visitOp The operator for which all immediate downstream 
     * operators should be returned.
     * @param graph The graph JSONObject in which {@code visitOp} resides.
     * @return A list of all operators immediately downstream from {@code visitOp}
     */
    static ArrayList<JSONObject> getDownstream(JSONObject visitOp,
            JSONObject graph) {
        ArrayList<JSONObject> uniqueChildren = new ArrayList<JSONObject>();
        HashSet<JSONObject> children = new HashSet<JSONObject>();
        JSONArray outputs = (JSONArray) visitOp.get("outputs");
        if (outputs == null || outputs.size() == 0) {
            return uniqueChildren;
        }

        for (int i = 0; i < outputs.size(); i++) {
            JSONArray connections = (JSONArray) ((JSONObject) outputs.get(i))
                    .get("connections");
            for (int j = 0; j < connections.size(); j++) {
                String inputPort = (String) connections.get(j);
                // TODO: build index instead of iterating through graph each
                // time
                JSONArray ops = (JSONArray) graph.get("operators");
                for (int k = 0; k < ops.size(); k++) {
                    JSONArray inputs = (JSONArray) ((JSONObject) ops.get(k))
                            .get("inputs");
                    if (inputs != null && inputs.size() != 0) {
                        for (int l = 0; l < inputs.size(); l++) {
                            String name = (String) ((JSONObject) inputs.get(l))
                                    .get("name");
                            if (name.equals(inputPort)) {
                                children.add((JSONObject) ops.get(k));
                            }
                        }
                    }
                }
            }
        }
        uniqueChildren.addAll(children);
        return uniqueChildren;
    }

    /**
     * Get all operators immediately upstream of the {@code visitOp}
     * operator. No exceptions are made for marker operators. 
     * <br><br>
     * Specifically, searches the graph, and returns a list of operators which 
     * have an output port connected to any input port of {@code visitOp}.
     * @param visitOp The operator for which all immediate upstream 
     * operators should be returned.
     * @param graph The graph JSONObject in which {@code visitOp} resides.
     * @return A list of all operators immediately upstream from {@code visitOp}
     */
    static List<JSONObject> getUpstream(JSONObject visitOp,
            JSONObject graph) {
        List<JSONObject> uniqueParents = new ArrayList<>();
        Set<JSONObject> parents = new HashSet<>();
        JSONArray inputs = (JSONArray) visitOp.get("inputs");
        if (inputs == null || inputs.size() == 0) {
            return uniqueParents;
        }
        for (int i = 0; i < inputs.size(); i++) {
            JSONArray connections = (JSONArray) ((JSONObject) inputs.get(i))
                    .get("connections");
            for (int j = 0; j < connections.size(); j++) {
                String outputPort = (String) connections.get(j);
                // TODO: build index instead of iterating through graph each
                // time
                JSONArray ops = (JSONArray) graph.get("operators");
                for (int k = 0; k < ops.size(); k++) {
                    JSONArray outputs = (JSONArray) ((JSONObject) ops.get(k))
                            .get("outputs");
                    if (outputs != null && outputs.size() != 0) {
                        for (int l = 0; l < outputs.size(); l++) {
                            String name = (String) ((JSONObject) outputs.get(l))
                                    .get("name");
                            if (name.equals(outputPort)) {
                                parents.add((JSONObject) ops.get(k));
                            }
                        }
                    }
                }
            }
        }
        uniqueParents.addAll(parents);
        return uniqueParents;
    }

    static void removeOperators(List<JSONObject> operators,
            JSONObject graph) {
        for (JSONObject iso : operators) {

            // Get parents and children of operator
            List<JSONObject> operatorParents = GraphUtilities.getUpstream(iso,
                    graph);
            List<JSONObject> operatorChildren = GraphUtilities.getDownstream(iso,
                    graph);

            
            JSONArray operatorOutputs = (JSONArray) iso.get("outputs");
            
            // Get the output name of the operator
            String operatorOutName="";
            if(operatorOutputs != null){
                JSONObject operatorFirstOutput = (JSONObject) operatorOutputs
                        .get(0);
                if(operatorFirstOutput != null){
                    operatorOutName = (String) operatorFirstOutput.get("name");
                }
            }
            
            // Also get input name
            String operatorInName="";
            JSONArray operatorInputs = (JSONArray) iso.get("inputs");
            if(operatorInputs != null){
                JSONObject operatorFirstInput = (JSONObject) operatorInputs.get(0);
                if(operatorFirstInput != null){
                    operatorInName = (String) operatorFirstInput.get("name");
                }
            }

            // Respectively, the names of the child and parent input and
            // output ports connected to the operator.
            List<String> childInputPortNames = new ArrayList<>();
            List<String> parentOutputPortNames = new ArrayList<>();

            // References to the list of connections for the parent and child
            // output and input ports that are connected to the $isolate$
            // operator.
            List<JSONArray> childConnections = new ArrayList<>();
            List<JSONArray> parentConnections = new ArrayList<>();

            // Get names of children's input ports that are connected to the
            // $Isolate$ operator;
            for (JSONObject child : operatorChildren) {
                JSONArray inputs = (JSONArray) child.get("inputs");
                for (Object inputObj : inputs) {
                    JSONObject input = (JSONObject) inputObj;
                    JSONArray connections = (JSONArray) input
                            .get("connections");
                    for (Object connectionObj : connections) {
                        String connection = (String) connectionObj;
                        if (connection.equals(operatorOutName)) {
                            childInputPortNames.add((String) input.get("name"));
                            childConnections.add(connections);
                            connections.remove(connection);
                            break;
                        }
                    }
                }
            }

            // Get names of parent's output ports that are connected to the
            // $Isolate$ operator;
            for (JSONObject parent : operatorParents) {
                JSONArray outputs = (JSONArray) parent.get("outputs");
                for (Object outputObj : outputs) {
                    JSONObject output = (JSONObject) outputObj;
                    JSONArray connections = (JSONArray) output
                            .get("connections");
                    for (Object connectionObj : connections) {
                        String connection = (String) connectionObj;
                        if (connection.equals(operatorInName)) {
                            parentOutputPortNames.add((String) output
                                    .get("name"));
                            parentConnections.add(connections);
                            connections.remove(connection);
                            break;
                        }
                    }
                }
            }

            // Connect child to parents
            for (JSONArray childConnection : childConnections) {
                childConnection.addAll(parentOutputPortNames);
            }

            // Connect parent to children
            for (JSONArray parentConnection : parentConnections) {
                parentConnection.addAll(childInputPortNames);
            }
            JSONArray ops = (JSONArray) graph.get("operators");
            ops.remove(iso);
        }
    }

    // Visits every node in the region defined by the boundaries, and applies
    // to it the consumer's accept() method.
    static void visitOnce(List<JSONObject> starts,
            List<String> boundaries, JSONObject graph,
            Consumer<JSONObject> consumer) {
        Set<JSONObject> visited = new HashSet<JSONObject>();
        List<JSONObject> unvisited = new ArrayList<JSONObject>();

        unvisited.addAll(starts);

        while (unvisited.size() > 0) {
            JSONObject op = unvisited.get(0);
            // Modify and THEN add to hashSet as to not break the hashCode of
            // the object in the hashSet.
            consumer.accept(op);
            visited.add(op);  
            GraphUtilities.getUnvisitedAdjacentNodes(visited, unvisited, op,
                    graph, boundaries);
            unvisited.remove(0);
            
  
        }
    }

    static void getUnvisitedAdjacentNodes(
            Collection<JSONObject> visited, Collection<JSONObject> unvisited,
            JSONObject op, JSONObject graph, List<String> boundaries) {
        
        List<JSONObject> parents = GraphUtilities.getUpstream(op, graph);
        List<JSONObject> children = GraphUtilities.getDownstream(op, graph);
        removeVisited(parents, visited);
        removeVisited(children, visited);

        // --- Process parents ---
        Set<JSONObject> allOperatorChildren = new HashSet<>();
        List<JSONObject> operatorParents = new ArrayList<>();
        for (JSONObject parent : parents) {
            if (equalsAny(boundaries, (String) parent.get("kind"))) {
                operatorParents.add(parent);
                allOperatorChildren.addAll(GraphUtilities.getDownstream(parent,
                        graph));
            }
        }
        visited.addAll(operatorParents);
        parents.removeAll(operatorParents);

        removeVisited(allOperatorChildren, visited);
        parents.addAll(allOperatorChildren);

        unvisited.addAll(parents);

        // --- Process children ---
        List<JSONObject> childrenToRemove = new ArrayList<JSONObject>();
        Set<JSONObject> allOperatorParents = new HashSet<>();
        for (JSONObject child : children) {
            if (equalsAny(boundaries, (String) child.get("kind"))) {
                childrenToRemove.add(child);
                allOperatorParents.addAll(GraphUtilities.getUpstream(child,
                        graph));
            }
        }
        visited.addAll(childrenToRemove);
        children.removeAll(childrenToRemove);

        removeVisited(allOperatorParents, visited);
        children.addAll(allOperatorParents);

        unvisited.addAll(children);
    }

    private static void removeVisited(Collection<JSONObject> ops,
            Collection<JSONObject> visited) {
        Iterator<JSONObject> it = ops.iterator();
        // Iterate in this manner to preserve list structure while deleting
        while (it.hasNext()) {
            JSONObject op = it.next();
            if (visited.contains(op)) {
                it.remove();
            }
        }
    }

    private static boolean equalsAny(List<String> boundaries, String opKind) {
        if(boundaries == null){
            return false;
        }
        
        for (String boundary : boundaries) {
            if (boundary.equals(opKind))
                return true;
        }
        return false;
    }    
}
