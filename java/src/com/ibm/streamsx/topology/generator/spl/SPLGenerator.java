/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.generator.spl;

import static com.ibm.streamsx.topology.builder.JParamTypes.TYPE_COMPOSITE_PARAMETER;
import static com.ibm.streamsx.topology.builder.JParamTypes.TYPE_SUBMISSION_PARAMETER;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.KIND;
import static com.ibm.streamsx.topology.generator.spl.GraphUtilities.getDownstream;
import static com.ibm.streamsx.topology.generator.spl.GraphUtilities.getUpstream;
import static com.ibm.streamsx.topology.generator.spl.GraphUtilities.kind;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.DEPLOYMENT_CONFIG;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.CFG_HAS_ISOLATE;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.CFG_HAS_LOW_LATENCY;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.CFG_STREAMS_COMPILE_VERSION;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.CFG_STREAMS_VERSION;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.splAppNamespace;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.array;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jboolean;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jobject;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.hasAny;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Stack;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.ibm.streamsx.topology.TStream.Routing;
import com.ibm.streamsx.topology.builder.BVirtualMarker;
import com.ibm.streamsx.topology.builder.JParamTypes;
import com.ibm.streamsx.topology.generator.operator.OpProperties;
import com.ibm.streamsx.topology.generator.port.PortProperties;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;

public class SPLGenerator {
    // Needed for composite name generation
    private int numComposites = 0;

    // The final list of composites (Main composite and parallel regions), which
    // compose the graph.
    List<JsonObject> composites = new ArrayList<>();
    
    private SubmissionTimeValue stvHelper;
    
    private int targetVersion;
    private int targetRelease;
    @SuppressWarnings("unused")
    private int targetMod;
    
    // List of physical operator fields indicating that the operator
    // is the start of a region
    List<String> compOperatorStarts = new ArrayList<>();
    
    // The kinds of the virtual operators that are the start of a region
    List<String> compStarts = new ArrayList<>();
    
    // The kinds of the virtual operators that are the end of a region.
    List<String> compEnds = new ArrayList<>();

    public String generateSPL(JsonObject graph) throws IOException {
        JsonObject graphConfig = getGraphConfig(graph);
        breakoutVersion(graphConfig);
                
        stvHelper = new SubmissionTimeValue(graph);
        new Preprocessor(this, graph).preprocess();
        
        separateIntoComposites(graph);
        
        //Make Main composite
        JsonObject mainCompsiteDef = new JsonObject();
        mainCompsiteDef.addProperty(KIND, graph.get("name").getAsString());
        mainCompsiteDef.addProperty("public", true);
        mainCompsiteDef.add("parameters", graph.get("parameters"));
        mainCompsiteDef.addProperty("__spl_mainComposite", true);
        mainCompsiteDef.add("operators", graph.get("operators"));
        composites.add(mainCompsiteDef);
        
        StringBuilder sb = new StringBuilder();
        generateGraph(graph, sb);
        
        setDeployment(graph);
        
        return sb.toString();
    }
    
    private void separateIntoComposites(JsonObject graph){

        compOperatorStarts.add(OpProperties.PARALLEL);
        compStarts.add(BVirtualMarker.PARALLEL.kind());
        compEnds.add(BVirtualMarker.END_PARALLEL.kind());
        
//        compOperatorStarts.add(null);
//        compStarts.add(BVirtualMarker.LOW_LATENCY.kind());
//        compEnds.add(BVirtualMarker.END_LOW_LATENCY.kind());
        

        // Find composites until there are no more to find.
        while(createComposite(graph)){

        }     
    }
    
    /**
     * Traverses the graph. If it finds a composite region, it creates its definition, invocation, and
     * inserts the invocation into the graph.
     * @param graph
     * @return true if a composite was found. False otherwise.
     */
    private boolean createComposite(JsonObject graph){
        // Try to find a composite of any type: low latency, parallel, etc.
        for(int i = 0; i < compStarts.size(); i++){
            List<List<JsonObject> > startsEndsAndOperators = findCompositeOpsOfAType(graph, compStarts.get(i), compEnds.get(i), compOperatorStarts.get(i));

            if(startsEndsAndOperators != null){
                
                JsonObject compDefinition;
                // Composite definition contains list of operators
                compDefinition = createCompositeDefinition(startsEndsAndOperators);
                JsonObject compInvocation;

                if(compStarts.get(i).equals(BVirtualMarker.PARALLEL.kind())){                  
                    compInvocation = createParallelCompositeInvocation(compDefinition, startsEndsAndOperators);
                }
                else if(compStarts.get(i).equals(BVirtualMarker.LOW_LATENCY.kind())){
                    compInvocation = createLowLatencyCompositeInvocation(compDefinition, startsEndsAndOperators);                  
                }
                else{
                    throw new IllegalStateException("Unsupported composite type: " + compStarts.get(i));
                } 
                
                // Fix the naming of the operators in the composite to read from the composite input ports
                // and write to the composite output ports
                fixCompositeInputNaming(graph, startsEndsAndOperators, compDefinition);
                fixCompositeOutputNaming(graph, startsEndsAndOperators, compDefinition, compInvocation);
                
                // Add the invocation to the graph
                array(graph, "operators").add(compInvocation);

                // Remove starts, ends, and composite operators from the graph
                for(int j = 0; j < startsEndsAndOperators.size(); j++){
                    for(JsonElement op : startsEndsAndOperators.get(j)){
                        array(graph, "operators").remove(op);
                    }
                }
                
                // Add the composite to the list of composites
                stvHelper.addJsonParamDefs(compDefinition);
                stvHelper.addJsonInstanceParams(compInvocation, compDefinition);
                
                composites.add(compDefinition);
                
                return true;
            }
        }
        
        return false;
    }


    /**
     * When we create a composite, operators need to create connections with the composite's input port.
     * @param graph
     * @param startsEndsAndOperators
     * @param opDefinition
     */
    private void fixCompositeInputNaming(JsonObject graph, List<List<JsonObject>> startsEndsAndOperators,
            JsonObject opDefinition) {   
        // For each start
        // We iterate like this because we need to also index into the operatorDefinition's inputNames list.
        for(int i = 0; i <  startsEndsAndOperators.get(0).size(); i++){
            JsonObject start = startsEndsAndOperators.get(0).get(i);
            
            //If the start is a source, the name doesn't need fixing.
            // Only input ports that now connect to the Composite input need to be fixed.
            if(start.has("config") && (hasAny(object(start, "config"), compOperatorStarts)))
                continue;
            
            // Given its output port name
            // Region markers like $Parallel$ only have one input and output
            String outputPortName = GsonUtilities.jstring(start.get("outputs").getAsJsonArray().get(0).getAsJsonObject(), "name");
            
            // for each operator downstream from this start
            for(JsonObject downstream : GraphUtilities.getDownstream(start, graph)){
                // for each input in the downstream operator
                JsonArray inputs = array(downstream, "inputs");
                for(JsonElement inputObj : inputs){
                    JsonObject input = inputObj.getAsJsonObject();
                    // for each connection in that input
                    JsonArray connections = array(input, "connections");
                    for(int j = 0; j < connections.size(); j++){
                        
                        // Replace the connection with the composite input port name if the 
                        // port has a connection to the start operator. 
                        if(connections.get(j).getAsString().equals(outputPortName)){
                            connections.set(j, GsonUtilities.array(opDefinition, "inputNames").get(j));
                        }
                    }
                }        
            }          
        }
        
        
    }
    
    
    /**
     * When we create a composite, operators need to create connections with the composite's output port.
     * @param graph
     * @param startsEndsAndOperators
     * @param opDefinition
     */
    private void fixCompositeOutputNaming(JsonObject graph, List<List<JsonObject>> startsEndsAndOperators,
            JsonObject opDefinition, JsonObject opInvocation) {   
        // We iterate like this because we need the index into the operatorDefinition's inputNames list.
        for(int[] i = {0}; i[0] <  startsEndsAndOperators.get(1).size(); i[0]++){
            JsonObject end = startsEndsAndOperators.get(1).get(i[0]);
            
            // Region markers like $Parallel$ only have one input and output
            String inputPortName = GsonUtilities.jstring(end.get("inputs").getAsJsonArray().get(0).getAsJsonObject(), "name");
            
            for(JsonObject parent : getUpstream(end, graph)){
                if (jstring(parent, "kind").equals("com.ibm.streamsx.topology.functional.java::HashAdder")) {
                    
                    String endType = jstring(array(end, "outputs").get(0).getAsJsonObject(), "type");
                    array(opInvocation, "outputs").get(0).getAsJsonObject().addProperty("type", endType);
                }
                
                GraphUtilities.outputs(parent, output -> {
                    JsonArray conns = array(output, "connections");
                    for(JsonElement conn : conns){
                        String sconn = conn.getAsString();
                        if(sconn.equals(inputPortName)){
                            output.addProperty("name", GsonUtilities.array(opDefinition, "outputNames").get(i[0]).getAsString());
                        }
                    }
                });
            }
            
        }
    }

    private JsonObject createCompositeDefinition(List<List<JsonObject>> startsEndsAndOperators) {        
        // Create a composite with a kind, input names, and output names  
        JsonObject compositeDefinition = new JsonObject();
        String compositeKind = "Composite" + this.numComposites++;
        
        compositeDefinition.addProperty(KIND, compositeKind);
        compositeDefinition.addProperty("public", false);
        
        // Add operators
        JsonArray operators = new JsonArray();
        for(JsonObject obj : startsEndsAndOperators.get(2))
            operators.add(obj);
        
        // If it's a physical composite start operator, add it.
        for(JsonObject obj : startsEndsAndOperators.get(0))
            if(obj.has("config") && (hasAny(object(obj, "config"), compOperatorStarts)))
                operators.add(obj);
            
        compositeDefinition.add("operators", operators);
        
        // Create input names
        JsonArray inputNames = new JsonArray();
        for(int i = 0; i < startsEndsAndOperators.get(0).size(); i++){
            JsonObject start = startsEndsAndOperators.get(0).get(i);
            // If it's not a source operator
            if(!(start.has("config") && (hasAny(object(start, "config"), compOperatorStarts)))){
                inputNames.add(new JsonPrimitive("__In" + i));
            }
        }
        compositeDefinition.add("inputNames", inputNames);
        
        // Create output names
        JsonArray outputNames = new JsonArray();
        for(int i = 0; i < startsEndsAndOperators.get(1).size(); ++i)
            outputNames.add(new JsonPrimitive("__Out" + i));
        compositeDefinition.add("outputNames", outputNames);
        
        return compositeDefinition;
    }
    
    private JsonObject createCompositeInvocation(JsonObject opDefinition, List<List<JsonObject>> startsEndsAndOperators) {
        JsonObject compositeInvocation = new JsonObject();

        // Create name and kind
        compositeInvocation.addProperty(KIND, jstring(opDefinition, KIND));
        String parallelCompositeName = jstring(opDefinition, KIND) + "Invocation";
        compositeInvocation.addProperty("name", parallelCompositeName);
        
        // Create the inputs of the invocation -- what streams it consumes
        JsonArray inputs = new JsonArray();
        for(JsonObject startOp : startsEndsAndOperators.get(0)){
            GraphUtilities.inputs(startOp, input -> inputs.add(input));     
        } 
        compositeInvocation.add("inputs", inputs);
        
        // Create the outputs of the invocation
        JsonArray outputs = new JsonArray();
        for(JsonObject endOp : startsEndsAndOperators.get(1)){
            GraphUtilities.outputs(endOp, output -> outputs.add(output));
        } 
        compositeInvocation.add("outputs", outputs);
        
        return compositeInvocation;
    }

    private JsonObject createParallelCompositeInvocation(JsonObject opDefinition, List<List<JsonObject>> startsEndsAndOperators) {
        JsonObject compositeInvocation = createCompositeInvocation(opDefinition, startsEndsAndOperators);
        
        // Create object with parallel information of input ports
        JsonObject parallelInfo = new JsonObject();
        JsonArray broadcastPorts = new JsonArray();
        JsonArray partitionedPorts = new JsonArray();
        
        parallelInfo.add("broadcastPorts", broadcastPorts);
        parallelInfo.add("partitionedPorts", partitionedPorts);
        
        for(JsonObject startOp : startsEndsAndOperators.get(0)){
            if(startOp.has("config") && startOp.get("config").getAsJsonObject().has(OpProperties.WIDTH)){
                JsonElement width = startOp.get("config").getAsJsonObject().get(OpProperties.WIDTH);
                parallelInfo.add(OpProperties.WIDTH, width);
            }
            
            // If it's a physical source start operator, ignore its output port, it has no partition information.
            if(startOp.has("config") && (hasAny(object(startOp, "config"), compOperatorStarts)))
                continue;
            
            // Otherwise, get the partition information.
            JsonObject outputPort = array(startOp, "outputs").get(0).getAsJsonObject();
            JsonObject inputPort = array(startOp, "inputs").get(0).getAsJsonObject();
            
            // Set the width if it was contained in the output port.
            if(outputPort.has(OpProperties.WIDTH))
                parallelInfo.add(OpProperties.WIDTH, outputPort.get(OpProperties.WIDTH));
            
            
            if(jstring(outputPort, PortProperties.ROUTING).equals(Routing.BROADCAST.name())){
                broadcastPorts.add(inputPort.get("name"));
            }
            
            if(outputPort.has(PortProperties.PARTITIONED) && jboolean(outputPort, PortProperties.PARTITIONED)){
                JsonObject partitionInfo = new JsonObject();
                partitionInfo.add("name", inputPort.get("name"));
                partitionInfo.add(PortProperties.PARTITION_KEYS, outputPort.get(PortProperties.PARTITION_KEYS));
                partitionedPorts.add(partitionInfo);
                
                // There is at least one partitioned port, therefore it is partitioned.
                compositeInvocation.addProperty("partitioned", true);
            }      
                
        } 
        
        compositeInvocation.add("parallelInfo", parallelInfo);
        compositeInvocation.addProperty("parallelOperator", true);
        return compositeInvocation;
    }

    private JsonObject createLowLatencyCompositeInvocation(JsonObject opDefinition, List<List<JsonObject>> startsEndsAndOperators) {
        JsonObject compositeInvocation = createCompositeInvocation(opDefinition, startsEndsAndOperators);
        
        return compositeInvocation;
    }

    private List<List<JsonObject> > findCompositeOpsOfAType(JsonObject graph, String startKind, String endKind, String opStartParam){
        
        for(JsonElement jePotentialStart : graph.getAsJsonArray("operators")){
            JsonObject potentialStart = jePotentialStart.getAsJsonObject();
            
            // We've found a potential start to a composite. See if the composite doesn't contain another composite.   
            if(kind(potentialStart).equals(startKind) || 
                    potentialStart.has("config") && jboolean(object(potentialStart, "config"), opStartParam)){
                List<List<JsonObject> > startsEndsAndOperators = findCompositeOpsOfATypeGivenPotentialStart(graph, startKind, endKind, opStartParam, potentialStart);
                if (startsEndsAndOperators != null) {
                    return startsEndsAndOperators;
                }
            }
            
        }
        return null;
    }
    
    private List<List<JsonObject>> findCompositeOpsOfATypeGivenPotentialStart(JsonObject graph, String startKind, String endKind, String opStartParam, JsonObject potentialStart){
        Stack<JsonObject> unvisited = new Stack<>();
        
        // Operators we've visited before
        List<JsonObject> visited = new ArrayList<>();
        
        // The potential start operators, end operators, and operators of the composite
        List<JsonObject> potStarts = new ArrayList<>(), potEnds = new ArrayList<>(), potOperators = new ArrayList<>();      
        
        unvisited.push(potentialStart);
        while(unvisited.size() > 0){
            JsonObject op = unvisited.pop();
            visited.add(op);
            Set<JsonObject> parents = new HashSet<>(), children = new HashSet<>();
            // Add the op to one of the lists containing the composite's operators

            if(kind(op).equals(startKind) || (op.has("config") && jboolean(object(op, "config"), opStartParam))){
                potStarts.add(op);
                children.addAll(getDownstream(op, graph));
            }
            else if(kind(op).equals(endKind)){
                potEnds.add(op);
                parents.addAll(getUpstream(op,graph));
            }
            else{
                potOperators.add(op);
                children.addAll(getDownstream(op, graph));
                parents.addAll(getUpstream(op,graph));
            }
            
            // Remove ops we've seen before
            // and ops that are already scheduled to be visited
            children.removeIf(pOp -> visited.contains(pOp));
            parents.removeIf(pOp -> visited.contains(pOp)); 
            children.removeIf(pOp -> unvisited.contains(pOp));
            parents.removeIf(pOp -> unvisited.contains(pOp));
            
            // Validate neighbors.
            
            // Check to see if the children are 
            // 1. end markers of a different type. If so, then this graph is invalid.
            // 2. start markers of any type, in which case we need to search for another composite
            //    because this one contains another composite.
            for(JsonObject cOp : children){
                if(compEnds.contains(kind(cOp)) && !kind(cOp).equals(endKind)){
                    // Throw an error if regions of a different type overlap
                    throw new IllegalStateException("Cannot have overlapping regions of different types.");
                }
                
                if(compStarts.contains(kind(cOp))){
                    // Composite contains another composite.
                    return null;
                }                
            }
            
            // Check to see if parent is:
            // 1. a start marker of a different type. If so, then this graph is invalid.
            // 2. an end marker of any type, in which case we need to search for another composite
            //    because this one contains another composite.
            for(JsonObject pOp : parents){
                // if parents is not empty, and the op is an end op then fail.
                // There should only ever be one input to the end of a composite.
                //
                // In other words, since the current operator is an end operator,
                // and since there is at least one parent in the unvisited list,
                // then it means that there are at least two inputs to the end
                // of the composite.
                if(kind(op).equals(endKind)){
                    throw new IllegalStateException("Cannot invoke union() before ending a region.");
                }
                
                // If the parent is a start operator of a different kind,
                // then there are overlapping regions.
                if((compStarts.contains(kind(pOp)) && !kind(pOp).equals(startKind)) ||
                        op.has("config") && (hasAny(object(op, "config"), compOperatorStarts) && !object(op, "config").has(opStartParam))){
                       // Throw an error if regions of a different type overlap
                       throw new IllegalStateException("Cannot have overlapping regions of different types.");
                }
                
                if(compEnds.contains(pOp)){
                    // Composite contains another composite.
                    return null;
                } 
            }         
            unvisited.addAll(parents);
            unvisited.addAll(children);

        }
        
        // IE, a parallel region can't just be .parallel().endparallel()
        if(potOperators.size() == 0){
            throw new IllegalStateException("A region must contain at least one operator.");
        }
       
        
        List<List<JsonObject> > startsStopsAndOperators = new ArrayList<>();
        startsStopsAndOperators.add(potStarts);
        startsStopsAndOperators.add(potEnds);
        startsStopsAndOperators.add(potOperators);
        
        return startsStopsAndOperators;      
    }
    
    
    /**
     * Set any Job Config Overlay deployment options
     * based upon the graph.
     * Currently always sets fusion scheme legacy
     * to ensure that isolation works.
     */
    private void setDeployment(JsonObject graph) {
        
        JsonObject config = jobject(graph, "config");
                      
        // DeploymentConfig
        JsonObject deploymentConfig = new JsonObject();
        config.add(DEPLOYMENT_CONFIG, deploymentConfig);
        
        boolean hasIsolate = jboolean(config, CFG_HAS_ISOLATE);
        boolean hasLowLatency = jboolean(config, CFG_HAS_LOW_LATENCY);
        
        if (hasIsolate)     
            deploymentConfig.addProperty("fusionScheme", "legacy");
        else {
            
            // Default to isolating parallel channels.
            JsonObject parallelRegionConfig = new JsonObject();
            deploymentConfig.add("parallelRegionConfig", parallelRegionConfig);
            
            parallelRegionConfig.addProperty("fusionType", "channelIsolation");
        }
    }
    
    void generateGraph(JsonObject graph, StringBuilder sb) throws IOException {
        JsonObject graphConfig = getGraphConfig(graph);
        graphConfig.addProperty("supportsJobConfigOverlays", versionAtLeast(4,2));

        String namespace = splAppNamespace(graph);
        if (namespace != null && !namespace.isEmpty()) {
            sb.append("namespace ");
            sb.append(namespace);
            sb.append(";\n");
        }

        

        for (int i = 0; i < composites.size(); i++) {
            StringBuilder compBuilder = new StringBuilder();
            generateComposite(graphConfig, composites.get(i), compBuilder);
            sb.append(compBuilder.toString());
        }
    }
    
    private void breakoutVersion(JsonObject graphConfig) {
        String version = jstring(graphConfig, CFG_STREAMS_COMPILE_VERSION);
        if (version == null) {
            version = jstring(graphConfig, CFG_STREAMS_VERSION);
            if (version == null)
                version = "4.0.1";
        }
        String[] vrmf = version.split("\\.");
        targetVersion = Integer.valueOf(vrmf[0]);
        targetRelease = Integer.valueOf(vrmf[1]);
        // allow version to be only V.R (e.g. 4.2)
        if (vrmf.length > 2)
            targetMod = Integer.valueOf(vrmf[2]);
    }
    
    boolean versionAtLeast(int version, int release) {
        if (targetVersion > version)
            return true;
        if (targetVersion == version)
            return targetRelease >= release;
        return false;
    }

    void generateComposite(JsonObject graphConfig, JsonObject graph,
            StringBuilder compBuilder) throws IOException {
        boolean isPublic = jboolean(graph, "public");
        String kind = jstring(graph, KIND);
        kind = getSPLCompatibleName(kind);
        if (isPublic)
            compBuilder.append("public ");

        compBuilder.append("composite ");

        compBuilder.append(kind);
        if (kind.startsWith("Composite")) {
            JsonArray inputNames = array(graph, "inputNames");
            JsonArray outputNames = array(graph, "outputNames");
            compBuilder.append("(");
            if(inputNames!= null && inputNames.size() > 0){
                compBuilder.append("input ");
                boolean first = true;
                for(JsonElement inputName : inputNames){
                    if(!first)
                        compBuilder.append(",");
                    String strInputName = getSPLCompatibleName(inputName.getAsString());
                    compBuilder.append(strInputName);
                    first = false;
                }
                
            }
            
            if(outputNames!= null && outputNames.size() > 0){
                if(inputNames!= null && inputNames.size() > 0)
                    compBuilder.append(";");
                compBuilder.append("output ");
                boolean first = true;
                for(JsonElement outputName : outputNames){
                    if(!first)
                        compBuilder.append(",");
                    String strOutputName = getSPLCompatibleName(outputName.getAsString());
                    compBuilder.append(strOutputName); 
                    first = false;
                }
                
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
    
    private void generateCompParams(JsonObject graph, StringBuilder sb) {
        JsonObject jparams = GsonUtilities.jobject(graph, "parameters");
        if (jparams != null && jparams.entrySet().size() > 0) {
            sb.append("param\n");
            for (Entry<String, JsonElement> on : jparams.entrySet()) {
                String name = on.getKey();
                JsonObject param = on.getValue().getAsJsonObject();
                String type = jstring(param, "type");
                
                if (TYPE_COMPOSITE_PARAMETER.equals(type)) {
                    JsonObject value = param.get("value").getAsJsonObject();
                    
                    sb.append("  ");
                    String metaType = jstring(value, "metaType");
                    String splType = Types.metaTypeToSPL(metaType);
                    
                    sb.append(String.format("expression<%s> $%s", splType, name));
                    if (value.has("defaultValue")) {
                        sb.append(" : ");
                        sb.append(value.get("defaultValue").getAsString());
                    }
                        
                    sb.append(";\n");
                }
                else if (TYPE_SUBMISSION_PARAMETER.equals(type))
                    ; // ignore - as it was converted to a TYPE_COMPOSITE_PARAMETER
                else
                    throw new IllegalArgumentException("Unhandled param name=" + name + " jo=" + param);
            }
        }
    }
    
    private void generateCompConfig(JsonObject graph, JsonObject graphConfig, StringBuilder sb) {
        boolean isMainComposite = jboolean(graph, "__spl_mainComposite");
        if (isMainComposite) {
            generateMainCompConfig(graphConfig, sb);
        }
    }
    
    private void generateMainCompConfig(JsonObject graphConfig, StringBuilder sb) {
        JsonArray hostPools = array(graphConfig, "__spl_hostPools");
        boolean hasHostPools =  hostPools != null && hostPools.size() != 0;
        
        JsonObject checkpoint = GsonUtilities.jobject(graphConfig, "checkpoint");
        
        boolean hasCheckpoint = checkpoint != null;
                
        if (hasHostPools || hasCheckpoint)
            sb.append("  config\n");
        
        
        if (hasHostPools) {
            boolean seenOne = false;
            for (JsonElement hpo : hostPools) {
                if (!seenOne) {
                    sb.append("    hostPool:\n");
                    seenOne = true;
                } else {
                    sb.append(",");
                }
                JsonObject hp = hpo.getAsJsonObject();
                String name = jstring(hp, "name");
                JsonArray resourceTags = array(hp, "resourceTags");
                
                sb.append("    ");
                sb.append(name);
                sb.append("=createPool({tags=[");
                for (int i = 0; i < resourceTags.size(); i++) {
                    if (i != 0)
                        sb.append(",");
                    stringLiteral(sb, resourceTags.get(i).getAsString());
                }
                sb.append("]}, Sys.Shared)");
            }
            sb.append(";\n");
        }
        
        if (hasCheckpoint) {
            TimeUnit unit = TimeUnit.valueOf(jstring(checkpoint, "unit"));
            long period = checkpoint.get("period").getAsLong();
            
            // SPL works in seconds, including fractions.
            long periodMs = unit.toMillis(period);
            double periodSec = ((double) periodMs) / 1000.0;
            sb.append("    checkpoint: periodic(");
            sb.append(periodSec);
            sb.append(");\n");
        }
    }

    void operators(JsonObject graphConfig, JsonObject graph, StringBuilder sb)
            throws IOException {
      
        OperatorGenerator opGenerator = new OperatorGenerator(this);
        JsonArray ops = array(graph, "operators");
        for (JsonElement ope : ops) {
            String splOp = opGenerator.generate(graphConfig, ope.getAsJsonObject());
            sb.append(splOp);
            sb.append("\n");
        }
    }
    
    SubmissionTimeValue stvHelper() {
        return stvHelper;
    }

    /**
     * Takes a name String that might have characters which are incompatible in
     * an SPL stream name (which just supports ASCII) and returns a valid SPL
     * name.
     * 
     * In addition since an operator name maps to a file name (with .cpp etc. suffixes)
     * we limit the name to a reasonable length. Any name that cannot be represented
     * as ASCII in under 80 characters is mapped to a MD5 representation of
     * the name.
     * 
     * This is a one way mapping, we only need to provide a name that is a
     * unique and consistent mapping of the input.
     * 
     * Use of MD5 means hashing and thus a really small chance of collisions
     * for different names.
     * 
     * Since the true (user) name can be set in a SPL note annotation
     * and displayed by the console, having a "meaningless" name is
     * not so much of an issue.
     * 
     * @param name
     * @return A string which can be a valid SPL stream name. If name is valid
     * as an SPL identifier and less than 80 chars then it is returned (same reference).
     */
    private static final int NAME_LEN = 80;
    public static String getSPLCompatibleName(String name) {

        if (name.length() <= NAME_LEN && name.matches("^[a-zA-Z_][a-zA-Z0-9_]+$"))
            return name;
        
        final byte[] original = name.getBytes(StandardCharsets.UTF_8);
        return "__spl_" + md5Name(original);
    }
    public static String md5Name(byte[] original) {
        try {
            
            MessageDigest md = MessageDigest.getInstance("MD5");
            StringBuilder sb = new StringBuilder(32);
            for (byte b : md.digest(original))
                sb.append(String.format("%02x", b));
                      
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            // Java is required to have MD5
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Add an arbitrary SPL value.
     * JsonObject has a type and a value. 
     */
    static void value(StringBuilder sb, JsonObject tv) {
        
        JsonElement value = tv.get("value");
        
        String type = JParamTypes.TYPE_SPL_EXPRESSION;
        if (tv.has("type")) {
            type = tv.get("type").getAsString();          
        } else {
            if (value.isJsonPrimitive()) {
                JsonPrimitive pv = value.getAsJsonPrimitive();               
                if (pv.isString())
                    type = "RSTRING";
            }
            else if (value.isJsonArray()) {
                type = "RSTRING";
            }
        }
               
        if (value.isJsonArray()) {
            JsonArray array = value.getAsJsonArray();
            
           for (int i = 0; i < array.size(); i++) {
                if (i != 0)
                    sb.append(", ");
                value(sb, type, array.get(i));
            }
        }
        else
        {
            value(sb, type, value);
        }
    }
    
    /**
     * Add a single value of a known type.
     */
    static void value(StringBuilder sb, String type, JsonElement value) {
        switch (type) {
        case "UINT8":
        case "UINT16":
        case "UINT32":
        case "UINT64":
        case "INT8":
        case "INT16":
        case "INT32":
        case "INT64":
        case "FLOAT32":
        case "FLOAT64":
            numberLiteral(sb, value.getAsJsonPrimitive(), type);
            break;
        case "RSTRING":
            stringLiteral(sb, value.getAsString());
            break;
        case "USTRING":
            stringLiteral(sb, value.getAsString());
            sb.append("u");
            break;
            
        case "BOOLEAN":
            sb.append(value.getAsBoolean());
            break;
            
        default:
        case JParamTypes.TYPE_ENUM:
        case JParamTypes.TYPE_SPLTYPE:
        case JParamTypes.TYPE_ATTRIBUTE:
        case JParamTypes.TYPE_SPL_EXPRESSION:
            sb.append(value.getAsString());
            break;
        }
    }

    
    static String stringLiteral(String value) {
        StringBuilder sb = new StringBuilder();
        stringLiteral(sb, value);
        return sb.toString();
    }

    /**
     * Use single quotes for strings to allow clearer
     * representation of JSON objects.
     */
    static void stringLiteral(StringBuilder sb, String value) {
        sb.append("'");

        // Replace any backslash with an escaped version
        // to stop SPL treating the value as an escape leadin
        value = value.replace("\\", "\\\\");

        // Replace new-lines with its SPL escaped version, \n
        // which is \\n as a Java string literal
        value = value.replace("\n", "\\n");

        value = value.replace("'", "\\'");

        sb.append(value);
        sb.append("'");
    }

    /**
     * Append the value with the correct SPL suffix. Integer & Double do not
     * require a suffix
     */
    static void numberLiteral(StringBuilder sb, JsonPrimitive value, String type) {
        String suffix = "";
        
        switch (type) {
        case "INT8": suffix = "b"; break;
        case "INT16": suffix = "h"; break;
        case "INT32": break;
        case "INT64": suffix = "l"; break;
        
        case "UINT8": suffix = "ub"; break;
        case "UINT16": suffix = "uh"; break;
        case "UINT32": suffix = "uw"; break;
        case "UINT64": suffix = "ul"; break;
        
        case "FLOAT32": suffix = "w"; break; // word, meaning 32 bits
        case "FLOAT64": break;
        }

        String literal;

        if (value.isNumber() && isUnsignedInt(type)) {
            Number nv = value.getAsNumber();

            if ("UINT64".equals(type))
                literal = Long.toUnsignedString(nv.longValue());
            else if ("UINT32".equals(type))
                literal = Integer.toUnsignedString(nv.intValue());
            else if ("UINT16".equals(type))
                literal = Integer.toUnsignedString(Short.toUnsignedInt(nv.shortValue()));
            else
                literal = Integer.toUnsignedString(Byte.toUnsignedInt(nv.byteValue()));
        } else {
            literal = value.getAsNumber().toString();
        }
        
        sb.append(literal);
        sb.append(suffix);
    }
    
    private static boolean isUnsignedInt(String type) {
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

    static JsonObject getGraphConfig(JsonObject graph) {
        return GsonUtilities.objectCreate(graph, "config");
    }
}
