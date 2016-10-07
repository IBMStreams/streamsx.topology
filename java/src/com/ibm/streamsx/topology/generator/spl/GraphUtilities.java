/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.generator.spl;

import static com.ibm.streamsx.topology.generator.spl.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.generator.spl.GsonUtilities.objectArray;
import static com.ibm.streamsx.topology.generator.spl.GsonUtilities.stringArray;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.builder.BVirtualMarker;
import com.ibm.streamsx.topology.function.Consumer;

public class GraphUtilities {
    static Set<JSONObject> findStarts(JSONObject graph) {
        Set<JSONObject> starts = new HashSet<>();
        JSONArray ops = (JSONArray) graph.get("operators");
        for (Object _op : ops) {
            JSONObject op = (JSONObject) _op;
            JSONArray inputs = (JSONArray) op.get("inputs");
            if (inputs == null || inputs.isEmpty()) {
                if (op.get("name") != null
                        && !((String) op.get("name"))
                                .startsWith("$")) {
                    starts.add(op);
                }
            }
        }
        return starts;
    }

    static Set<JSONObject> findOperatorByKind(BVirtualMarker virtualMarker,
            JSONObject _graph) {
        
        JsonObject graph = gson(_graph);
        Set<JsonObject> kindOperators = new HashSet<>();
        
        operators(graph, op -> {
            if (virtualMarker.isThis(jstring(op, "kind")))
                kindOperators.add(op);
        });

        return backToJSON(_graph, kindOperators);
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
    static Set<JSONObject> getDownstream(JSONObject visitOp,
            JSONObject graph) {
        Set<JSONObject> children = new HashSet<>();
        JSONArray outputs = (JSONArray) visitOp.get("outputs");
        if (outputs == null || outputs.isEmpty()) {
            return children;
        }

        for (Object _out : outputs) {
            JSONArray connections = (JSONArray) ((JSONObject) _out)
                    .get("connections");
            for (Object _conn : connections) {
                String inputPort = (String) _conn;
                // TODO: build index instead of iterating through graph each
                // time
                JSONArray ops = (JSONArray) graph.get("operators");
                for (Object _op : ops) {
                    JSONObject op = (JSONObject) _op;
                    JSONArray inputs = (JSONArray) op.get("inputs");
                    if (inputs != null && !inputs.isEmpty()) {
                        for (Object _input : inputs) {
                            String name = (String) ((JSONObject) _input).get("name");
                            if (name.equals(inputPort)) {
                                children.add(op);
                            }
                        }
                    }
                }
            }
        }
        return children;
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
    public static Set<JSONObject> getUpstream(JSONObject _visitOp,
            JSONObject _graph) {
        Set<JsonObject> parents = new HashSet<>();
        
        JsonObject graph = gson(_graph);
        JsonObject visitOp = gson(_visitOp);
        
        inputConnections(visitOp, outputPort -> {
            operators(graph, op -> outputs(op, output-> {
                if (jstring(output, "name").equals(outputPort))
                    parents.add(op);
            }));
        });

        return backToJSON(_graph, parents);
    }
    
    /**
     * Copies operator, giving it a new name. Also renames input/output ports,
     * and clears the input/output connections array.
     * @param op
     * @param name
     */
    static JSONObject copyOperatorNewName(JSONObject op, String name){
        JSONObject op_new=null;
        try {
            op_new = JSONObject.parse(op.serialize());
        } catch (IOException e) {
            throw new RuntimeException("Error copying operator " + (String)op.get("name"), e);
        }
        op_new.put("name", name);
        @SuppressWarnings("unchecked")
        Collection<JSONObject> inputs = (Collection<JSONObject>)op_new.get("inputs");
        @SuppressWarnings("unchecked")
        Collection<JSONObject> outputs = (Collection<JSONObject>)op_new.get("outputs");
        for(JSONObject input : inputs){
            input.put("name", name + "_IN" + input.get("index").toString());
            JSONArray conns = (JSONArray) input.get("connections");
            conns.clear();
        }
        for(JSONObject output : outputs){
            output.put("name", name + "_OUT" + output.get("index").toString());
            JSONArray conns = (JSONArray) output.get("connections");
            conns.clear();
        }
        return op_new;
    }

    static void removeOperator(JSONObject op, JSONObject graph){
        removeOperators(Collections.singletonList(op), graph);
    }

    static void removeOperators(Collection<JSONObject> operators,
            JSONObject graph) {
        for (JSONObject iso : operators) {

            // Get parents and children of operator
            Set<JSONObject> operatorParents = GraphUtilities.getUpstream(iso,
                    graph);
            Set<JSONObject> operatorChildren = GraphUtilities.getDownstream(iso,
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
            
            // Also get input names
            List<String> operatorInNames = new ArrayList<>();
            JSONArray operatorInputs = (JSONArray) iso.get("inputs");
            if(operatorInputs != null){
            	for(Object _input : operatorInputs) {
            		JSONObject in = (JSONObject) _input;
                    operatorInNames.add((String) in.get("name"));
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
            // operator;
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
                        if(operatorInNames.contains(connection)) {	
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
    
    public enum Direction {UPSTREAM, DOWNSTREAM, BOTH};
    
    public static class VisitController {
        private final Direction direction;
        private final Set<BVirtualMarker> markerBoundaries;
        private boolean stop = false;
        /** default is DOWNSTREAM */
        public VisitController() {
            this(Direction.DOWNSTREAM, null);
        }
        public VisitController(Direction direction) {
            this(direction, null);
        }
        public VisitController(Direction direction, Set<BVirtualMarker> markerBoundaries) {
            this.direction = direction;
            if (markerBoundaries == null)
                markerBoundaries = Collections.emptySet();
            this.markerBoundaries = markerBoundaries;
        }
        public Direction direction() { return direction; }
        public Set<BVirtualMarker> markerBoundaries() { return markerBoundaries; }
        public boolean stopped() { return stop; }
        public void setStop() { stop = true; }
    }

    // Visits every node in the region defined by the boundaries, and applies
    // to it the consumer's accept() method.
    static void visitOnce(Set<JSONObject> starts,
            Set<BVirtualMarker> boundaries, JSONObject graph,
            Consumer<JSONObject> consumer) {
        visitOnce(new VisitController(Direction.BOTH, boundaries),
                starts, graph, consumer);
    }

    /**
     * Starting with {@code starts} nodes, visit every node in the specified
     * direction and apply the consumer's {@code accept()} method.
     * <p>
     * Don't call {@code accept()} for a
     * {@code visitController.markerBounderies()}
     * node and cease traversal of a branch if such an node is encountered.
     * <p>
     * During traversal, return if {@code visitController.getStop()==true}.
     * @param visitController may be null; defaults is Direction.DOWNSTREAM
     * @param starts
     * @param graph
     * @param consumer
     */
    public static void visitOnce(VisitController visitController,
            Set<JSONObject> starts, JSONObject graph,
            Consumer<JSONObject> consumer) {
        Set<JSONObject> visited = new HashSet<>();
        List<JSONObject> unvisited = new ArrayList<>();
        if (visitController == null)
            visitController = new VisitController();


        unvisited.addAll(starts);

        while (unvisited.size() > 0) {
            JSONObject op = unvisited.get(0);
            // Modify and THEN add to hashSet as to not break the hashCode of
            // the object in the hashSet.
            if (visitController.stopped())
                return;
            consumer.accept(op);
            visited.add(op);  
            GraphUtilities.getUnvisitedAdjacentNodes(visitController, visited,
                    unvisited, op, graph);
            unvisited.remove(0);
        }
    }

    static void getUnvisitedAdjacentNodes(
            Collection<JSONObject> visited, Collection<JSONObject> unvisited,
            JSONObject op, JSONObject graph, Set<BVirtualMarker> boundaries) {
        getUnvisitedAdjacentNodes(new VisitController(Direction.BOTH, boundaries),
                visited, unvisited, op, graph);
    }

    static void getUnvisitedAdjacentNodes(
            VisitController visitController,
            Collection<JSONObject> visited, Collection<JSONObject> unvisited,
            JSONObject op, JSONObject graph) {
        
        Direction direction = visitController.direction();
        Set<BVirtualMarker> boundaries = visitController.markerBoundaries();
        
        Set<JSONObject> parents = GraphUtilities.getUpstream(op, graph);
        Set<JSONObject> children = GraphUtilities.getDownstream(op, graph);
        removeVisited(parents, visited);
        removeVisited(children, visited);

        // --- Process parents ---
        if (direction != Direction.DOWNSTREAM) {
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
        }

        // --- Process children ---
        if (direction != Direction.UPSTREAM) {
            List<JSONObject> childrenToRemove = new ArrayList<>();
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

    private static boolean equalsAny(Set<BVirtualMarker> boundaries, String opKind) {
        if(boundaries == null){
            return false;
        }
        
        for (BVirtualMarker boundary : boundaries) {
            if (boundary.isThis(opKind))
                return true;
        }
        return false;
    }

    static String getInputPortName(JSONObject op, int index) {
        JSONArray inputs = (JSONArray) op.get("inputs");
        JSONObject input = (JSONObject) inputs.get(index);
        return (String) input.get("name");
    }    
    
    static String getOutputPortName(JSONObject op, int index) {
        JSONArray outputs = (JSONArray) op.get("outputs");
        JSONObject output = (JSONObject) outputs.get(index);
        return (String) output.get("name");
    }  
    
    static void addBefore(JSONObject op, JSONObject addOp, JSONObject graph){
        for(JSONObject parent : getUpstream(op, graph)){
            addBetween(parent, op, addOp);
        }       
    }
    
    static void addBetween(JSONObject parent, JSONObject child, JSONObject op){
        List<JSONObject> parentList = new ArrayList<>();
        List<JSONObject> childList = new ArrayList<>();
        parentList.add(parent);
        childList.add(child);
        
        addBetween(parentList, childList, op);     
    }
    
    @SuppressWarnings("unchecked")
	static void addBetween(List<JSONObject> parents, List<JSONObject> children, JSONObject op){
        for(JSONObject parent : parents){
            for(JSONObject child : children){              
                JSONArray outputs = (JSONArray) parent.get("outputs");
                JSONArray inputs = (JSONArray) child.get("inputs");
                for(JSONObject output : (Collection<JSONObject>)outputs){
                    for(JSONObject input : (Collection<JSONObject>)inputs){
                        insertOperatorBetweenPorts(input, output, op);
                    }
                }              
            }
        }
    }
    
    @SuppressWarnings("unchecked")
	static void insertOperatorBetweenPorts(JSONObject input, JSONObject output, JSONObject op){
        String oportName = (String) output.get("name");
        String iportName = (String) input.get("name");
        
        JSONObject opInput = (JSONObject) ((JSONArray)op.get("inputs")).get(0);
        JSONObject opOutput = (JSONObject) ((JSONArray)op.get("outputs")).get(0);
        
        String opIportName = (String) opInput.get("name");
        String opOportName = (String) opOutput.get("name");
        
        // Attach op in inputs and outputs
        JSONArray opInputConns = (JSONArray) opInput.get("connections");
        JSONArray opOutputConns = (JSONArray) opOutput.get("connections");
        if(!opInputConns.contains(oportName)){
            opInputConns.add(oportName);
        }
        if(!opOutputConns.add(iportName)){
            opOutputConns.add(iportName);
        }
        
        JSONArray outputConns = (JSONArray) output.get("connections");
        JSONArray inputConns = (JSONArray) input.get("connections");
        
        for(String conn : (Collection<String>)outputConns){
            if(conn.equals(iportName)){
                outputConns.set(outputConns.indexOf(conn), opIportName);
            }
        }
        
        for(String conn : (Collection<String>)inputConns){
            if(conn.equals(oportName)){
                inputConns.set(inputConns.indexOf(conn), opOportName);
            }
        }
    }
    
    /**
     * Perform an action for every operator in the graph
     */
    static void operators(JsonObject graph, Consumer<JsonObject> action) {
        objectArray(graph, "operators", action);
    }
    
    /**
     * Perform an action for every input for an operator
     */
    static void inputs(JsonObject op, Consumer<JsonObject> action) {
        objectArray(op, "inputs", action);
    }
    
    static void inputConnections(JsonObject op, Consumer<String> action) {
        inputs(op, input -> stringArray(input, "connections", action));
    }
    
    /**
     * Perform an action for every output for an operator
     */
    static void outputs(JsonObject op, Consumer<JsonObject> action) {
        objectArray(op, "outputs", action);
    }
    
    /**
     * TEMP
     * 
     */
    
    static JsonObject gson(JSONObject object) {
        try {
            return new JsonParser().parse(object.serialize()).getAsJsonObject();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    static Set<JSONObject> backToJSON(JSONObject graph, Set<JsonObject> ops) {
        Set<JSONObject> _ops = new HashSet<>();
        Set<String> names = new HashSet<>();
        for (JsonObject op : ops) {
            names.add(GsonUtilities.jstring(op, "name"));
        }
        
        JSONArray opsa = (JSONArray) graph.get("operators");
        for (Object opo : opsa) {
            JSONObject op = (JSONObject) opo;
            if (names.contains(op.get("name")))
                _ops.add(op);          
        }
        
        return _ops;
    }
}
