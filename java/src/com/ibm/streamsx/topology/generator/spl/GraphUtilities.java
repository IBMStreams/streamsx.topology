package com.ibm.streamsx.topology.generator.spl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.builder.BVirtualMarker;
import com.ibm.streamsx.topology.function.Consumer;

public class GraphUtilities {
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

    static ArrayList<JSONObject> findOperatorByKind(BVirtualMarker virtualMarker,
            JSONObject graph) {
        ArrayList<JSONObject> kindOperators = new ArrayList<JSONObject>();
        JSONArray ops = (JSONArray) graph.get("operators");
        for (int k = 0; k < ops.size(); k++) {
            JSONObject op = (JSONObject) (ops.get(k));
            if (virtualMarker.isThis((String) op.get("kind"))) {
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
    public static List<JSONObject> getUpstream(JSONObject visitOp,
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
    static void visitOnce(List<JSONObject> starts,
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
            List<JSONObject> starts, JSONObject graph,
            Consumer<JSONObject> consumer) {
        Set<JSONObject> visited = new HashSet<JSONObject>();
        List<JSONObject> unvisited = new ArrayList<JSONObject>();
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
        
        List<JSONObject> parents = GraphUtilities.getUpstream(op, graph);
        List<JSONObject> children = GraphUtilities.getDownstream(op, graph);
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
        List<JSONObject> parents = getUpstream(op, graph);
        for(JSONObject parent : (Collection<JSONObject>)parents){
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
}
