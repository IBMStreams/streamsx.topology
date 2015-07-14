/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.generator.spl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.json.java.OrderedJSONObject;
import com.ibm.streamsx.topology.builder.GraphBuilder;
import com.ibm.streamsx.topology.function7.Consumer;
import com.ibm.streamsx.topology.generator.spl.GraphUtilities;

public class SPLGenerator {
    // Needed for composite name generation
    private int numParallelComposites = 0;
    private int colocationCount = 0;
    private int lowLatencyRegionCount = 0;

    // The final list of composites (Main composite and parallel regions), which
    // compose the graph.
    ArrayList<JSONObject> composites = new ArrayList<JSONObject>();

    public String generateSPL(GraphBuilder graph) throws IOException {
        return generateSPL(graph.complete());
    }

    public String generateSPL(JSONObject graph) throws IOException {

        tagIsolationRegions(graph);
        tagLowLatencyRegions(graph);
        preProcessThreadedPorts(graph);
        removeUnionOperators(graph);
       
        // Generate parallel composites
        JSONObject comp = new OrderedJSONObject();
        comp.put("name", graph.get("name"));
        comp.put("public", true);

        ArrayList<JSONObject> starts = GraphUtilities.findStarts(graph);
        separateIntoComposites(starts, comp, graph);
        StringBuilder sb = new StringBuilder();
        generateGraph(graph, sb);
        return sb.toString();
    }
    
    // At this point, the $Union$ operators in the graph are just place holders.
    private void removeUnionOperators(JSONObject graph){
        List<JSONObject> unionOps = GraphUtilities.findOperatorByKind(GraphBuilder.UNION, graph);
        GraphUtilities.removeOperators(unionOps, graph);
    }
   
    @SuppressWarnings("serial")
    private void preProcessThreadedPorts(final JSONObject graph){
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

                List<JSONObject> parents = GraphUtilities.getParents(op, graph);
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
    

    void generateGraph(JSONObject graph, StringBuilder sb) throws IOException {

        String namespace = (String) graph.get("namespace");
        if (namespace != null && !namespace.isEmpty()) {
            sb.append("namespace ");
            sb.append(namespace);
            sb.append(";\n");
        }

        JSONObject graphConfig = getGraphConfig(graph);

        for (int i = 0; i < composites.size(); i++) {
            StringBuilder compBuilder = new StringBuilder();
            generateComposite(graphConfig, composites.get(i), compBuilder);
            sb.append(compBuilder.toString());
        }
    }

    void generateComposite(JSONObject graphConfig, JSONObject graph,
            StringBuilder compBuilder) throws IOException {
        Boolean isPublic = (Boolean) graph.get("public");
        String name = (String) graph.get("name");
        name = getSPLCompatibleName(name);
        if (isPublic != null && isPublic)
            compBuilder.append("public ");

        compBuilder.append("composite ");

        compBuilder.append(name);
        if (name.startsWith("__parallel_")) {
            String iput = (String) graph.get("inputName");
            String oput = (String) graph.get("outputName");

            iput = splBasename(iput);
            oput = splBasename(oput);

            compBuilder.append("(input " + iput + "; output " + oput + ")");
        }
        compBuilder.append("\n{\n");

        compBuilder.append("graph\n");
        operators(graphConfig, graph, compBuilder);

        compBuilder.append("}\n");
    }

    void operators(JSONObject graphConfig, JSONObject graph, StringBuilder sb)
            throws IOException {
        JSONArray ops = (JSONArray) graph.get("operators");

        if (ops == null || ops.isEmpty())
            return;

        for (int i = 0; i < ops.size(); i++) {
            JSONObject op = (JSONObject) ops.get(i);

            String splOp = OperatorGenerator.generate(graphConfig, op);
            sb.append(splOp);
            sb.append("\n");

        }
    }

    /**
     * Recursively breaks the graph into different composites, separating the
     * Main composite from the parallel ones. Should work with nested
     * parallelism, but hasn't yet been tested.
     * 
     * @param starts
     *            A list of operators that indicate the start of the region.
     *            These are either source operators, or operators in a composite
     *            that read from the composite's input port.
     * @param comp
     *            A JSON object representing a composite with a name field, but
     *            not an operator field.
     * @param graph
     *            The top-level JSON graph that contains all the operators.
     *            Necessary to pass it to the GraphUtilities.getChildren
     *            function.
     */
    JSONObject separateIntoComposites(ArrayList<JSONObject> starts,
            JSONObject comp, JSONObject graph) {
        // Contains all ops which have been reached by graph traversal,
        // regardless of whether they are 'special' operators, such as the ones
        // whose kind begins with '$', or whether they're included in the final
        // physical graph.
        HashSet<JSONObject> allTraversedOps = new HashSet<JSONObject>();

        // Only contains operators that are in the final physical graph.
        List<JSONObject> visited = new ArrayList<JSONObject>();

        // Operators which might not have been visited yet.
        List<JSONObject> unvisited = new ArrayList<JSONObject>();
        JSONObject unparallelOp = null;

        unvisited.addAll(starts);

        // While there are still nodes to visit
        while (unvisited.size() > 0) {
            // Get the first unvisited node
            JSONObject visitOp = unvisited.get(0);
            // Check whether we've seen it before. Remember, allTraversedOps
            // contains *every* operator we've traversed in the JSON graph,
            // while visited is a list of only the physical operators that will
            // be included in the graph.
            if (allTraversedOps.contains(visitOp)) {
                unvisited.remove(0);
                continue;
            }
            // We've now traversed this operator.
            allTraversedOps.add(visitOp);

            // If the operator is not a special operator, add it to the
            // visited list.
            if (!isParallelStart(visitOp) && !isParallelEnd(visitOp)) {
                ArrayList<JSONObject> children = GraphUtilities.getChildren(
                        visitOp, graph);
                unvisited.addAll(children);
                visited.add(visitOp);
            }

            // If the operator is the start of a parallel region, make a new
            // JSON
            // operator to insert into the main graph, make a new JSON graph to
            // represent the parallel composite, find the parallel region's
            // start
            // operators, and recursively call this function to populate the new
            // composite.
            else if (isParallelStart(visitOp)) {
                // The new composite, represented in JSON
                JSONObject subComp = new OrderedJSONObject();
                // The operator to include in the graph that refers to the
                // parallel composite.
                JSONObject compOperator = new OrderedJSONObject();
                subComp.put(
                        "name",
                        "__parallel_Composite_"
                                + Integer.toString(numParallelComposites));
                subComp.put("public", false);

                compOperator.put(
                        "kind",
                        "__parallel_Composite_"
                                + Integer.toString(numParallelComposites));
                compOperator.put("name",
                        "paraComp_" + Integer.toString(numParallelComposites));
                compOperator.put("inputs", visitOp.get("inputs"));

                Boolean partitioned = (Boolean) ((JSONObject) ((JSONArray) visitOp
                        .get("outputs")).get(0)).get("partitioned");
                if (partitioned != null && partitioned) {
                    JSONArray inputs = (JSONArray) visitOp.get("inputs");
                    String parallelInputPortName = null;

                    // Get the first port that has the __spl_hash attribute
                    for (int i = 0; i < inputs.size(); i++) {
                        JSONObject input = (JSONObject) inputs.get(i);
                        String type = (String) input.get("type");
                        if (type.contains("__spl_hash")) {
                            parallelInputPortName = (String) input.get("name");
                        }
                    }
                    compOperator.put("partitioned", true);
                    compOperator.put("parallelInputPortName",
                            parallelInputPortName);
                }

                // Necessary to later indicate whether the composite the
                // operator
                // refers to is parallelized.
                compOperator.put("parallelOperator", true);

                JSONArray outputs = (JSONArray) visitOp.get("outputs");
                JSONObject output = (JSONObject) outputs.get(0);
                compOperator.put("width", output.get("width"));
                numParallelComposites++;

                // Get the start operators in the parallel region -- the ones
                // immediately downstream from the $Parallel operator
                ArrayList<JSONObject> parallelStarts = GraphUtilities
                        .getChildren(visitOp, graph);

                // Once you have the start operators, recursively call the
                // function
                // to populate the parallel composite.
                JSONObject parallelEnd = separateIntoComposites(parallelStarts,
                        subComp, graph);

                // The name of the input port of the composite should be the
                // name of the input port of the first child operator. For now,
                // we're assuming that all start operators in a parallel region
                // all read from the same upstream port.
                subComp.put("inputName",
                        ((JSONArray) ((JSONObject) ((JSONArray) parallelStarts
                                .get(0).get("inputs")).get(0))
                                .get("connections")).get(0));

                // If the parallel region ended by invoking unparallel,
                // the output port of the invocation of the composite operator
                // should be the same as the output port of the $unparallel
                // operator
                if (parallelEnd != null) {
                    ArrayList<JSONObject> children = GraphUtilities
                            .getChildren(parallelEnd, graph);
                    unvisited.addAll(children);
                    compOperator.put("outputs", parallelEnd.get("outputs"));
                    subComp.put("outputName",
                            ((JSONArray) ((JSONObject) ((JSONArray) parallelEnd
                                    .get("inputs")).get(0)).get("connections"))
                                    .get(0));
                }

                // Add comp operator to the list of physical operators
                visited.add(compOperator);
            }

            // Is end of parallel region
            else {
                unparallelOp = visitOp;
            }

            // remove the operator we've traversed from the list of unvisited
            // operators.
            unvisited.remove(0);
        }

        JSONArray compOps = new JSONArray(visited.size());
        compOps.addAll(visited);

        comp.put("operators", compOps);
        composites.add(comp);

        // If one of the operators in the composite was the $unparallel operator
        // then return that $unparallel operator, otherwise return null.
        return unparallelOp;
    }

    @SuppressWarnings("serial")
    private void assignColocations(JSONObject isolate, List<JSONObject> starts,
            JSONObject graph) {

        final String colocationTag = "Colocation"
                + Integer.toString(colocationCount++);

        List<String> boundaries = new ArrayList<>();
        boundaries.add("$Isolate$");

        GraphUtilities.visitOnce(starts, boundaries, graph,
                new Consumer<JSONObject>() {

                    @Override
                    public void accept(JSONObject op) {
                        JSONObject config = (JSONObject) op.get("config");
                        if (config == null || config.isEmpty()) {
                            config = new OrderedJSONObject();
                            op.put("config", config);
                        }

                        // If the region has already been assigned a colocation
                        // tag, simply
                        // return.
                        String regionTag = (String) config.get("colocationTag");
                        if (regionTag != null && !regionTag.isEmpty()) {
                            return;
                        }
                        
                        config.put("colocationTag", colocationTag);
                    }

                });
    }

    /**
     * Determine whether any isolated region is ever joined with its parent.
     * I.E:
     * 
     * <pre>
     * <code>
     *       |---$Isolate---|
     *   ----|              |----
     *       |--------------|
     * </code>
     * </pre>
     * 
     * @param isolate
     *            An $Isolate$ operator in the graph
     * @return a boolean which is false if the the Isolated region is later
     *         merged with its parent.
     */
    @SuppressWarnings("serial")
    private void checkValidColocationRegion(JSONObject isolate, JSONObject graph) {
        final List<JSONObject> isolateChildren = GraphUtilities.getChildren(
                isolate, graph);
        List<JSONObject> isoParents = GraphUtilities.getParents(isolate, graph);

        assertNotIsolated(isoParents);

        List<String> boundaries = new ArrayList<>();
        boundaries.add("$Isolate$");

        GraphUtilities.visitOnce(isoParents, boundaries, graph,
                new Consumer<JSONObject>() {
                    @Override
                    public void accept(JSONObject op) {
                        if (isolateChildren.contains(op)) {
                            throw new IllegalStateException(
                                    "Invalid isolation "
                                            + "configuration. An isolated region is joined with a non-"
                                            + "isolated region.");
                        }
                    }
                });
    }

    private void tagIsolationRegions(JSONObject graph) {
        // Check whether graph is valid for colocations
        List<JSONObject> isolateOperators = GraphUtilities.findOperatorByKind(
                GraphBuilder.ISOLATE, graph);
        
        for (JSONObject jso : isolateOperators) {
            checkValidColocationRegion(jso, graph);
        }

        // Assign isolation regions their partition colocations
        for (JSONObject isolate : isolateOperators) {
            assignColocations(isolate,
                    GraphUtilities.getParents(isolate, graph), graph);
            assignColocations(isolate,
                    GraphUtilities.getChildren(isolate, graph), graph);
        }
 
        tagIslandIsolatedRegions(graph);
        GraphUtilities.removeOperators(isolateOperators, graph);
    }
    
    @SuppressWarnings("serial")
    private void tagIslandIsolatedRegions(JSONObject graph){
        List<JSONObject> starts = GraphUtilities.findStarts(graph);   
        
        for(JSONObject start : starts){
            final String colocationTag = "Colocation"
                    + Integer.toString(colocationCount++);
            
            String regionTag = (String) start.get("colocationTag");
            if (regionTag != null && !regionTag.isEmpty()) {
                continue;
            }
            
            List<JSONObject> startList = new ArrayList<JSONObject>();
            startList.add(start);
            
            List<String> boundaries = new ArrayList<>();
            boundaries.add("$Isolate$");
            
            GraphUtilities.visitOnce(startList, boundaries, graph,
                    new Consumer<JSONObject>() {
                        @Override
                        public void accept(JSONObject op) {
                            JSONObject config = (JSONObject) op.get("config");
                            if (config == null || config.isEmpty()) {
                                config = new OrderedJSONObject();
                                op.put("config", config);
                            }

                            // If the region has already been assigned a colocation
                            // tag, simply
                            // return.
                            String regionTag = (String) config.get("colocationTag");
                            if (regionTag != null && !regionTag.isEmpty()) {
                                return;
                            }
                            
                            config.put("colocationTag", colocationTag);
                        }
                    });           
        }
    }

    private static void assertNotIsolated(Collection<JSONObject> jsos) {
        for (JSONObject jso : jsos) {
            if ("$Isolate$".equals((String) jso.get("kind"))) {
                throw new IllegalStateException(
                        "Cannot put \"isolate\" regions immediately"
                                + " adjacent to each other. E.g -- .isolate().isolate()");
            }
        }
    }

    private void tagLowLatencyRegions(JSONObject graph) {
        List<JSONObject> lowLatencyStartOperators = GraphUtilities
                .findOperatorByKind(GraphBuilder.LOW_LATENCY, graph);
        List<JSONObject> lowLatencyEndOperators = GraphUtilities
                .findOperatorByKind(GraphBuilder.END_LOW_LATENCY, graph);

        // Assign isolation regions their lowLatency tag
        for (JSONObject llStart : lowLatencyStartOperators) {
            assignLowLatency(llStart,
                    GraphUtilities.getChildren(llStart, graph), graph);
        }

        List<JSONObject> allLowLatencyOps = new ArrayList<>();
        allLowLatencyOps.addAll(lowLatencyEndOperators);
        allLowLatencyOps.addAll(lowLatencyStartOperators);

        GraphUtilities.removeOperators(allLowLatencyOps, graph);
    }

    @SuppressWarnings("serial")
    private void assignLowLatency(JSONObject llStart,
            List<JSONObject> llStartChildren, JSONObject graph) {

        final String lowLatencyTag = "LowLatencyRegion"
                + Integer.toString(lowLatencyRegionCount++);

        List<String> boundaries = new ArrayList<>();
        boundaries.add("$LowLatency$");
        boundaries.add("$EndLowLatency$");

        GraphUtilities.visitOnce(llStartChildren, boundaries, graph,
                new Consumer<JSONObject>() {
                    @Override
                    public void accept(JSONObject op) {
                        // If the region has already been assigned a lowLatency
                        // tag, simply
                        // return.
                        String regionTag = (String) op.get("lowLatencyTag");
                        if (regionTag != null && !regionTag.isEmpty()) {
                            return;
                        }
                        op.put("lowLatencyTag", lowLatencyTag);
                    }
                });

    }

    private boolean isParallelEnd(JSONObject visitOp) {
        return visitOp.get("kind").equals("$Unparallel$");
    }

    private boolean isParallelStart(JSONObject visitOp) {
        // TODO Auto-generated method stub
        return visitOp.get("kind").equals("$Parallel$");
    }

    /**
     * Takes a name String that might have characters which are incompatible in
     * an SPL stream name (which just supports ASCII) and returns a valid SPL
     * name.
     * 
     * This is a one way mapping, we only need to provide a name that is a
     * unique mapping of the input.
     * 
     * @param name
     * @return A string which can be a valid SPL stream name.
     */
    static String getSPLCompatibleName(String name) {

        if (name.matches("^[a-zA-Z0-9_]+$"))
            return name;

        StringBuilder sb = new StringBuilder(name.length());
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if ((c >= '0' && c <= '9') || (c >= 'a' && c <= 'z')
                    || (c >= 'A' && c <= 'Z')) {
                sb.append(c);
                continue;
            }
            if (c == '_') {
                sb.append("__");
                continue;
            }
            sb.append("_u");
            String code = Integer.toHexString(c);
            if (code.length() < 4)
                sb.append("000".substring(code.length() - 1));
            sb.append(code);
        }

        return sb.toString();
    }

    static String basename(String name) {
        int i = name.lastIndexOf('.');
        if (i == -1)
            return name;

        return name.substring(i + 1);

    }

    static String splBasename(String name) {
        return getSPLCompatibleName(basename(name));
    }

    static void stringLiteral(StringBuilder sb, String value) {
        sb.append('"');

        // Replace any backslash with an escaped version
        // to stop SPL treating the value as an escape leadin
        value = value.replace("\\", "\\\\");

        // Replace new-lines with its SPL escaped version, \n
        // which is \\n as a Java string literal
        value = value.replace("\n", "\\n");

        value = value.replace("\"", "\\\"");

        sb.append(value);
        sb.append('"');
    }

    /**
     * Append the value with the correct SPL suffix. Integer & Double do not
     * require a suffix.
     */
    static void numberLiteral(StringBuilder sb, Number value) {
        sb.append(value);

        if (value instanceof Byte)
            sb.append('b');
        else if (value instanceof Short)
            sb.append('h');
        else if (value instanceof Long)
            sb.append('l');
        else if (value instanceof Float)
            sb.append("w"); // word, meaning 32 bits

    }

    static JSONObject getGraphConfig(JSONObject graph) {
        JSONObject config = (JSONObject) graph.get("config");
        if (config == null)
            config = new JSONObject();
        return config;

    }
}
