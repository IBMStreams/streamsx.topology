/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.generator.spl;

import static com.ibm.streamsx.topology.generator.operator.OpProperties.START_OP;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.array;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jboolean;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.objectArray;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.stringArray;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSyntaxException;
import com.ibm.streamsx.topology.builder.BVirtualMarker;
import com.ibm.streamsx.topology.function.Consumer;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;

public class GraphUtilities {
    static Set<JsonObject> findStarts(JsonObject graph) {
        Set<JsonObject> starts = new HashSet<>();
        
        operators(graph, op -> {
            JsonArray inputs = GsonUtilities.array(op, "inputs");
            if (inputs == null || inputs.size() == 0) {
                // should this be kind?
                String name = jstring(op, "name");
                
                if(name != null && !name.startsWith("$")) {
                    starts.add(op);
                    return;
                }
            }
            if (jboolean(op, START_OP))
                starts.add(op);
            });
        
        return starts;
    }

    /*
    static Set<JSONObject> findOperatorByKind(BVirtualMarker virtualMarker,
            JSONObject _graph) {
        
        return backToJSON(_graph, findOperatorByKind(virtualMarker, gson(_graph)));
    }
    */
    /**
     * Get the kind of an operator.
     */
    static String kind(JsonObject op) {
        String kind = jstring(op, "kind");
        assert kind != null;
        return kind;
    }
    /**
     * Is an operator a specific kind.
     */
    static boolean isKind(JsonObject op, String kind) {
        return kind.equals(kind(op));
    }
    
    /**
     * Add an operator parameter, replacing the existing value if it exists.
     * Handles the case where no parameters exist.
     */
    static void addOpParameter(JsonObject op, String name, JsonObject value) {
        JsonObject params = GsonUtilities.objectCreate(op, "parameters");
        params.add(name, value);
    }
    
    static Set<JsonObject> findOperatorByKind(BVirtualMarker virtualMarker,
                JsonObject graph) {

        Set<JsonObject> kindOperators = new HashSet<>();
        
        operators(graph, op -> {
            if (virtualMarker.isThis(kind(op)))
                kindOperators.add(op);
        });

        return kindOperators;
    }
    
    /**
     * Find all (non-virtual) operators of specific kinds (by string).
     */
    static Set<JsonObject> findOperatorsByKinds(final JsonObject graph, final Set<String> kinds) {

        Set<JsonObject> kindOperators = new HashSet<>();

        operators(graph, op -> {
            if (kinds.contains(kind(op)))
                kindOperators.add(op);
        });

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
    /*
    static Set<JSONObject> getDownstream(JSONObject _visitOp,
            JSONObject _graph) {
        
        return backToJSON(_graph, getDownstream(gson(_visitOp), gson(_graph)));
    }
    */
    
    static Set<JsonObject> getDownstream(JsonObject visitOp,
            JsonObject graph) {    
        
        Set<JsonObject> children = new HashSet<>();
                
        outputConnections(visitOp, inputPort -> {
            operators(graph, op -> inputs(op, input-> {
                if (jstring(input, "name").equals(inputPort))
                    children.add(op);
            }));
        });

        return children;
        /*
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
        */
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
    /*
    public static Set<JSONObject> getUpstream(JSONObject _visitOp,
            JSONObject _graph) {
        
        return backToJSON(_graph, getUpstream(gson(_visitOp), gson(_graph)));
    }
    */
        
    public static Set<JsonObject> getUpstream(JsonObject visitOp,
                JsonObject graph) {
        Set<JsonObject> parents = new HashSet<>();
                
        inputConnections(visitOp, outputPort -> {
            operators(graph, op -> outputs(op, output-> {
                if (jstring(output, "name").equals(outputPort))
                    parents.add(op);
            }));
        });

        return parents;
    }
    
    /**
     * Copies operator, giving it a new name. Also renames input/output ports,
     * and clears the input/output connections array.
     * @param op
     * @param name
     */
    static JsonObject copyOperatorNewName(JsonObject op, String name){
        JsonObject op_new;
        try {
            JsonParser parser = new JsonParser();
            op_new = parser.parse(op.toString()).getAsJsonObject();
        } catch (JsonSyntaxException e) {
            throw new RuntimeException("Error copying operator " + jstring(op, "name"), e);
        }
        op_new.addProperty("name", name);
        
        inputs(op_new, input -> {
            input.addProperty("name", name + "_IN" + jstring(input, "index"));
            input.add("connections", new JsonArray());
        });
        
        outputs(op_new, output -> {
            output.addProperty("name", name + "_OUT" + jstring(output, "index"));
            output.add("connections", new JsonArray());
        });
        
        return op_new;
    }

    static void removeOperator(JsonObject op, JsonObject graph){
        removeOperators(Collections.singleton(op), graph);
    }

    static void removeOperators(Collection<JsonObject> operators,
            JsonObject graph) {
        for (JsonObject iso : operators) {

            // Get parents and children of operator
            Set<JsonObject> operatorParents = GraphUtilities.getUpstream(iso,
                    graph);
            Set<JsonObject> operatorChildren = GraphUtilities.getDownstream(iso,
                    graph);

            
            JsonArray operatorOutputs = array(iso, "outputs");
            
            // Get the output name of the operator
            String operatorOutName="";
            if(operatorOutputs != null){
                JsonObject operatorFirstOutput = operatorOutputs.get(0).getAsJsonObject();
                if(operatorFirstOutput != null){
                    operatorOutName = jstring(operatorFirstOutput, "name");
                }
            }
            
            // Also get input names
            List<String> operatorInNames = new ArrayList<>();
            inputs(iso, input -> operatorInNames.add(jstring(input, "name")));

            // Respectively, the names of the child and parent input and
            // output ports connected to the operator.
            List<String> childInputPortNames = new ArrayList<>();
            List<String> parentOutputPortNames = new ArrayList<>();

            // References to the list of connections for the parent and child
            // output and input ports that are connected to the $isolate$
            // operator.
            List<JsonArray> childConnections = new ArrayList<>();
            List<JsonArray> parentConnections = new ArrayList<>();

            // Get names of children's input ports that are connected to the
            // operator;
            for (JsonObject child : operatorChildren) {
                JsonArray inputs = child.get("inputs").getAsJsonArray();
                for (JsonElement inputObj : inputs) {
                    JsonObject input = inputObj.getAsJsonObject();
                    JsonArray connections = input.get("connections").getAsJsonArray();
                    for (JsonElement connectionObj : connections) {
                        String connection = connectionObj.getAsString();
                        if (connection.equals(operatorOutName)) {
                            childInputPortNames.add(jstring(input, "name"));
                            childConnections.add(connections);
                            connections.remove(connectionObj);
                            break;
                        }
                    }
                }
            }

            // Get names of parent's output ports that are connected to the
            // $Isolate$ operator;
            for (JsonObject parent : operatorParents) {
                JsonArray outputs = parent.get("outputs").getAsJsonArray();
                for (JsonElement outputObj : outputs) {
                    JsonObject output = outputObj.getAsJsonObject();
                    JsonArray connections = output.get("connections").getAsJsonArray();
                    for (JsonElement connectionObj : connections) {
                        String connection = connectionObj.getAsString();
                        if(operatorInNames.contains(connection)) {	
                            parentOutputPortNames.add(jstring(output, "name"));
                            parentConnections.add(connections);
                            connections.remove(connectionObj);
                            break;
                        }
                    }
                }
            }

            // Connect child to parents
            for (JsonArray childConnection : childConnections) {
                for (String name : parentOutputPortNames)
                    childConnection.add(new JsonPrimitive(name));
            }

            // Connect parent to children
            for (JsonArray parentConnection : parentConnections) {
                for (String name : childInputPortNames)
                    parentConnection.add(new JsonPrimitive(name));
            }
            JsonArray ops = graph.get("operators").getAsJsonArray();
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
    static void visitOnce(Set<JsonObject> starts,
            Set<BVirtualMarker> boundaries, JsonObject graph,
            Consumer<JsonObject> consumer) {
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
            Set<JsonObject> starts, JsonObject graph,
            Consumer<JsonObject> consumer) {
        Set<JsonObject> visited = new HashSet<>();
        List<JsonObject> unvisited = new ArrayList<>();
        if (visitController == null)
            visitController = new VisitController();


        unvisited.addAll(starts);

        while (unvisited.size() > 0) {
            JsonObject op = unvisited.get(0);
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
            Collection<JsonObject> visited, Collection<JsonObject> unvisited,
            JsonObject op, JsonObject graph, Set<BVirtualMarker> boundaries) {
        getUnvisitedAdjacentNodes(new VisitController(Direction.BOTH, boundaries),
                visited, unvisited, op, graph);
    }

    static void getUnvisitedAdjacentNodes(
            VisitController visitController,
            Collection<JsonObject> visited, Collection<JsonObject> unvisited,
            JsonObject op, JsonObject graph) {
        
        Direction direction = visitController.direction();
        Set<BVirtualMarker> boundaries = visitController.markerBoundaries();
        
        Set<JsonObject> parents = GraphUtilities.getUpstream(op, graph);
        Set<JsonObject> children = GraphUtilities.getDownstream(op, graph);
        removeVisited(parents, visited);
        removeVisited(children, visited);

        // --- Process parents ---
        if (direction != Direction.DOWNSTREAM) {
            Set<JsonObject> allOperatorChildren = new HashSet<>();
            List<JsonObject> operatorParents = new ArrayList<>();
            for (JsonObject parent : parents) {
                if (equalsAny(boundaries, jstring(parent, "kind"))) {
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
            List<JsonObject> childrenToRemove = new ArrayList<>();
            Set<JsonObject> allOperatorParents = new HashSet<>();
            for (JsonObject child : children) {
                if (equalsAny(boundaries, jstring(child, "kind"))) {
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

    private static void removeVisited(Collection<JsonObject> ops,
            Collection<JsonObject> visited) {
        Iterator<JsonObject> it = ops.iterator();
        // Iterate in this manner to preserve list structure while deleting
        while (it.hasNext()) {
            JsonObject op = it.next();
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

    static String getInputPortName(JsonObject op, int index) {
        JsonArray inputs = op.get("inputs").getAsJsonArray();
        JsonObject input = inputs.get(index).getAsJsonObject();
        return jstring(input, "name");
    }    
    
    static String getOutputPortName(JsonObject op, int index) {
        JsonArray outputs = op.get("output").getAsJsonArray();
        JsonObject output = outputs.get(index).getAsJsonObject();
        return jstring(output, "name");

    }  
    
    /**
     * Add an operator before another operator.
     * @param op Operator the new operator is to be added before.
     * @param addOp Operator to be added
     * @param graph The graph.
     */
    static void addBefore(JsonObject op, JsonObject addOp, JsonObject graph){        
        for(JsonObject parent : getUpstream(op, graph)){
            addBetween(parent, op, addOp);
        } 
        graph.get("operators").getAsJsonArray().add(addOp);
    }
    
    static void addBetween(JsonObject parent, JsonObject child, JsonObject op){
        List<JsonObject> parentList = new ArrayList<>();
        List<JsonObject> childList = new ArrayList<>();
        parentList.add(parent);
        childList.add(child);
        
        addBetween(parentList, childList, op);     
    }
    
	static void addBetween(List<JsonObject> parents, List<JsonObject> children, JsonObject op){
        for(JsonObject parent : parents){
            for(JsonObject child : children){      
                JsonArray outputs = parent.get("outputs").getAsJsonArray();
                JsonArray inputs = child.get("inputs").getAsJsonArray();
                for(JsonElement output : outputs){
                    for(JsonElement input : inputs){
                        insertOperatorBetweenPorts(input.getAsJsonObject(), output.getAsJsonObject(), op);
                    }
                }              
            }
        }
    }
    

	static void insertOperatorBetweenPorts(JsonObject input, JsonObject output, JsonObject op){
        String oportName = jstring(output, "name");
        String iportName = jstring(input, "name");
        
        JsonObject opInput = op.get("inputs").getAsJsonArray().get(0).getAsJsonObject();
        JsonObject opOutput = op.get("outputs").getAsJsonArray().get(0).getAsJsonObject();
        
        String opIportName = jstring(opInput, "name");
        String opOportName = jstring(opOutput, "name");
        
        // Attach op in inputs and outputs
        JsonArray opInputConns = opInput.get("connections").getAsJsonArray();
        JsonArray opOutputConns = opOutput.get("connections").getAsJsonArray();
        boolean add = true;
        for (JsonElement conn : opInputConns)
            if (conn.getAsString().equals(oportName)) {
                add = false;
                break;
            }
        if (add)
            opInputConns.add(new JsonPrimitive(oportName));
        
        add = true;
        for (JsonElement conn : opOutputConns)
            if (conn.getAsString().equals(iportName)) {
                add = false;
                break;
            }
        opOutputConns.add(new JsonPrimitive(iportName));
        
        JsonArray outputConns = output.get("connections").getAsJsonArray();
        JsonArray inputConns =  input.get("connections").getAsJsonArray();
        
        for (int i = 0 ; i < outputConns.size(); i++) {
            if (outputConns.get(i).getAsString().equals(iportName))
                outputConns.set(i, new JsonPrimitive(opIportName));
        }
        
        for (int i = 0 ; i < inputConns.size(); i++) {
            if (inputConns.get(i).getAsString().equals(oportName))
                inputConns.set(i, new JsonPrimitive(opOportName));
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
    
    static void outputConnections(JsonObject op, Consumer<String> action) {
        outputs(op, output -> stringArray(output, "connections", action));
    }
}
