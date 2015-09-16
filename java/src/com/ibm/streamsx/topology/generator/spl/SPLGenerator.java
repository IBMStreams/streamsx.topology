/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.generator.spl;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.ibm.streamsx.topology.internal.core.SubmissionParameter.TYPE_SUBMISSION_PARAMETER;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.json.java.OrderedJSONObject;
import com.ibm.streamsx.topology.builder.BVirtualMarker;
import com.ibm.streamsx.topology.builder.JGraph;

public class SPLGenerator {
    // Needed for composite name generation
    private int numParallelComposites = 0;

    // The final list of composites (Main composite and parallel regions), which
    // compose the graph.
    ArrayList<JSONObject> composites = new ArrayList<JSONObject>();
    
    private SubmissionTimeValue stvHelper;

    public String generateSPL(JSONObject graph) throws IOException {
        
        stvHelper = new SubmissionTimeValue(graph);
        new Preprocessor(graph).preprocess();
       
        // Generate parallel composites
        JSONObject comp = new OrderedJSONObject();
        comp.put("name", graph.get("name"));
        comp.put("public", true);
        comp.put("parameters", graph.get("parameters"));
        comp.put("__spl_mainComposite", true);

        ArrayList<JSONObject> starts = GraphUtilities.findStarts(graph);
        separateIntoComposites(starts, comp, graph);
        StringBuilder sb = new StringBuilder();
        generateGraph(graph, sb);
        return sb.toString();
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
            compBuilder.append("(input " + iput);
            
            if(oput != null && !oput.isEmpty()){
                oput = splBasename(oput);
                compBuilder.append("; output " + oput);
            }
              compBuilder.append(")");
        }
        compBuilder.append("\n{\n");
        
        generateCompParams(graph, compBuilder);

        compBuilder.append("graph\n");
        operators(graphConfig, graph, compBuilder);
        
        generateCompConfig(graph, graphConfig, compBuilder);

        compBuilder.append("}\n");
    }
    
    private void generateCompParams(JSONObject graph, StringBuilder sb) {
        JSONObject jparams = (JSONObject) graph.get("parameters");
        if (jparams != null && jparams.size() > 0) {
            Boolean isMainComposite = (Boolean) graph.get("__spl_mainComposite");
            if (isMainComposite == null)
                isMainComposite = false;
            sb.append("param\n");
            for (Object on : jparams.keySet()) {
                String name = (String) on;
                JSONObject param = (JSONObject) jparams.get(name);
                Object type = param.get("type");
                Object value = param.get("value");
                if (TYPE_SUBMISSION_PARAMETER.equals(type)) {
                    sb.append("  ");
                    if (isMainComposite)
                        stvHelper.generateMainDef((JSONObject)value, sb);
                    else
                        stvHelper.generateInnerDef((JSONObject)value, sb);
                    sb.append(";\n");
                }
                else
                    throw new IllegalArgumentException("Unhandled param name=" + name + " jo=" + param);
            }
        }
    }
    
    private void generateCompConfig(JSONObject graph, JSONObject graphConfig, StringBuilder sb) {
        Boolean isMainComposite = (Boolean) graph.get("__spl_mainComposite");
        if (isMainComposite != null && isMainComposite) {
            generateMainCompConfig(graphConfig, sb);
        }
    }
    
    private void generateMainCompConfig(JSONObject graphConfig, StringBuilder sb) {
        JSONArray hostPools = (JSONArray) graphConfig.get("__spl_hostPools");
        boolean hasHostPools =  hostPools != null && !hostPools.isEmpty();
        
        JSONObject checkpoint = (JSONObject) graphConfig.get("checkpoint");
        
        boolean hasCheckpoint = checkpoint != null;
                
        if (hasHostPools || hasCheckpoint)
            sb.append("  config\n");
        
        
        if (hasHostPools) {
            boolean seenOne = false;
            for (Object hpo : hostPools) {
                if (!seenOne) {
                    sb.append("    hostPool:\n");
                    seenOne = true;
                } else {
                    sb.append(",");
                }
                JSONObject hp = (JSONObject) hpo;
                String name = (String) hp.get("name");
                JSONArray resourceTags = (JSONArray) hp.get("resourceTags");
                
                sb.append("    ");
                sb.append(name);
                sb.append("=createPool({tags=[");
                for (int i = 0; i < resourceTags.size(); i++) {
                    if (i != 0)
                        sb.append(",");
                    stringLiteral(sb, resourceTags.get(i).toString());
                }
                sb.append("]}, Sys.Shared)");
            }
            sb.append(";\n");
        }
        
        if (hasCheckpoint) {
            TimeUnit unit = TimeUnit.valueOf(checkpoint.get("unit").toString());
            long period = Long.valueOf(checkpoint.get("period").toString());
            
            // SPL works in seconds, including fractions.
            long periodMs = unit.toMillis(period);
            double periodSec = ((double) periodMs) / 1000.0;
            sb.append("    checkpoint: periodic(");
            sb.append(periodSec);
            sb.append(");\n");
        }
    }

    void operators(JSONObject graphConfig, JSONObject graph, StringBuilder sb)
            throws IOException {
        JSONArray ops = (JSONArray) graph.get("operators");

        if (ops == null || ops.isEmpty())
            return;
        
        OperatorGenerator opGenerator = new OperatorGenerator(this);

        for (int i = 0; i < ops.size(); i++) {
            JSONObject op = (JSONObject) ops.get(i);

            String splOp = opGenerator.generate(graphConfig, op);
            sb.append(splOp);
            sb.append("\n");

        }
    }
    
    SubmissionTimeValue stvHelper() {
        return stvHelper;
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
                ArrayList<JSONObject> children = GraphUtilities.getDownstream(
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
                        .getDownstream(visitOp, graph);

                // Once you have the start operators, recursively call the
                // function
                // to populate the parallel composite.
                JSONObject parallelEnd = separateIntoComposites(parallelStarts,
                        subComp, graph);
                stvHelper.addJsonInstanceParams(compOperator, subComp);

                // Set all relevant input port connections to the input port
                // name of the parallel composite
                String parallelStartOutputPortName = (String)(output.get("name"));
                subComp.put("inputName", "parallelInput");
                for(JSONObject start : parallelStarts){
                    JSONArray inputs = (JSONArray) start.get("inputs");
                    for(Object inputObj : inputs){
                        JSONObject input = (JSONObject)inputObj;
                        JSONArray connections = (JSONArray) input.get("connections");
                        for(int i = 0; i < connections.size(); i++){
                            if(((String)connections.get(i)).equals(parallelStartOutputPortName)){
                                connections.set(i, "parallelInput");
                            }
                        }
                    }
                }

                if (parallelEnd != null) {
                    ArrayList<JSONObject> children = GraphUtilities
                            .getDownstream(parallelEnd, graph);
                    unvisited.addAll(children);
                    compOperator.put("outputs", parallelEnd.get("outputs"));
                    subComp.put("outputName", "parallelOutput");

		    // Set all relevant output port names to the output port of the
                    // parallel composite.
                    JSONObject paraEndIn = (JSONObject)((JSONArray)parallelEnd.get("inputs")).get(0);
                    String parallelEndInputPortName = (String)(paraEndIn.get("name"));
                    List<JSONObject> parallelOutParents = GraphUtilities.getUpstream(parallelEnd, graph);
                    for(JSONObject end : parallelOutParents){
			if(((String)end.get("kind")).equals("com.ibm.streamsx.topology.functional.java::HashAdder")){
			    String endType = (String)((JSONObject)((JSONArray)end.get("outputs")).get(0)).get("type");
			    ((JSONObject)((JSONArray)compOperator.get("outputs")).get(0)).put("type", endType);
			}
                        JSONArray parallelOutputs = (JSONArray) end.get("outputs");
                        for(Object outputObj : parallelOutputs){
                            JSONObject paraOutput = (JSONObject)outputObj;
                            JSONArray connections = (JSONArray) paraOutput.get("connections");
                            for(int i = 0; i < connections.size(); i++){
                                if(((String)connections.get(i)).equals(parallelEndInputPortName)){
                                    paraOutput.put("name", "parallelOutput");
                                }
                            }
                        }
                    }

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
        stvHelper.addJsonParamDefs(comp);
        composites.add(comp);

        // If one of the operators in the composite was the $unparallel operator
        // then return that $unparallel operator, otherwise return null.
        return unparallelOp;
    }




    private boolean isParallelEnd(JSONObject visitOp) {
        return BVirtualMarker.END_PARALLEL.isThis((String) visitOp.get("kind"));
    }

    private boolean isParallelStart(JSONObject visitOp) {
        return BVirtualMarker.PARALLEL.isThis((String) visitOp.get("kind"));
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

    
    static String stringLiteral(String value) {
        StringBuilder sb = new StringBuilder();
        stringLiteral(sb, value);
        return sb.toString();
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
     * require a suffix
     */
    static void numberLiteral(StringBuilder sb, Number value, Object type) {
        Object val = value;
        String suffix = "";
        boolean isUnsignedInt = isUnsignedInt(type); 

        if (value instanceof Byte)
            suffix = "b";
        else if (value instanceof Short)
            suffix = "h";
        else if (value instanceof Integer) {
            if (isUnsignedInt)
                suffix = "w";  // word, meaning 32 bits
        } else if (value instanceof Long)
            suffix = "l";
        else if (value instanceof Float)
            suffix = "w"; // word, meaning 32 bits

        if (isUnsignedInt) {
            val = unsignedString(value);
            suffix = "u" + suffix;
        }

        sb.append(val);
        sb.append(suffix);
    }
    
    private static boolean isUnsignedInt(Object type) {
        return "UINT8".equals(type)
                || "UINT16".equals(type)
                || "UINT32".equals(type)
                || "UINT64".equals(type);
    }
    
    /**
     * Get the string value of an "unsigned" Byte, Short, Integer or Long.
     */
    public static String unsignedString(Object integerValue) {
// java8 impl
//        if (integerValue instanceof Long)
//            return Long.toUnsignedString((Long) integerValue);
//        
//        Integer i;
//        if (integerValue instanceof Byte)
//            i = Byte.toUnsignedInt((Byte) integerValue);
//        else if (integerValue instanceof Short)
//            i = Short.toUnsignedInt((Short) integerValue);
//        else if (integerValue instanceof Integer)
//            i = (Integer) integerValue;
//        else
//            throw new IllegalArgumentException("Illegal type for unsigned " + integerValue.getClass());
//        return Integer.toUnsignedString(i);

        if (integerValue instanceof Long) {
            String hex = Long.toHexString((Long)integerValue);
            hex = "00" + hex;  // don't sign extend
            BigInteger bi = new BigInteger(hex, 16);
            return bi.toString();
        }

        long l;
        if (integerValue instanceof Byte)
            l = ((Byte) integerValue) & 0x00ff;
        else if (integerValue instanceof Short)
            l = ((Short) integerValue) & 0x00ffff;
        else if (integerValue instanceof Integer)
            l = ((Integer) integerValue) & 0x00ffffffffL;
        else
            throw new IllegalArgumentException("Illegal type for unsigned " + integerValue.getClass());
        return Long.toString(l);
    }

    static JSONObject getGraphConfig(JSONObject graph) {
        return JGraph.createConfig(graph);
    }
}
