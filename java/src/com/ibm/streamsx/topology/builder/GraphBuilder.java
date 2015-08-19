/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.builder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.streams.flow.declare.OperatorGraph;
import com.ibm.streams.flow.declare.OperatorGraphFactory;
import com.ibm.streams.operator.Operator;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.internal.functional.ops.PassThrough;

/**
 * Low-level graph builder. GraphBuilder provides a layer on top of
 * {@code com.ibm.streams.flow.OperatorGraph} to define a directed graph of
 * connected operators. Where possible information is maintained in the graph
 * declared by {@code OperatorGraph} but this class maintains additional
 * information in order to allow SPL generation (through JSON).
 * 
 * The graph can be converted to a JSON representation using {@link #complete()}
 * , which can then be used to generate SPL using
 * {@link com.ibm.streamsx.topology.generator.spl.SPLGenerator}.
 * 
 * If the graph only contains Java operators and functional operators, then it
 * may be executed using its {@code OperatorGraph} from {@link #graph()}.
 * 
 */
public class GraphBuilder extends BJSONObject {

    private final OperatorGraph graph = OperatorGraphFactory.newGraph();

    private final List<BOperator> ops = new ArrayList<>();
    
    private final JSONObject config = new JSONObject();

    public GraphBuilder(String namespace, String name) {
        super();

        json().put("namespace", namespace);
        json().put("name", name);
        json().put("public", true);
        json().put("config", config);
    }

   public BOperatorInvocation addOperator(Class<? extends Operator> opClass,
            Map<String, ? extends Object> params) {
        final BOperatorInvocation op = new BOperatorInvocation(this, opClass,
                params);
        ops.add(op);
         return op;
    }
   
   private final Map<String,Integer> usedNames = new HashMap<>();
   
   public BOperatorInvocation addOperator(
           String name,
           Class<? extends Operator> opClass,         
           Map<String, ? extends Object> params) {
       
       name = userSuppliedName(name);
       
       final BOperatorInvocation op = new BOperatorInvocation(this, name, opClass,
               params);
       ops.add(op);
        return op;
   }
   
   String userSuppliedName(String name) {
       if (usedNames.containsKey(name)) {
           int c = usedNames.get(name) + 1;
           usedNames.put(name, c);
           name = name + "_" + c;
       } else {
           usedNames.put(name, 1);
       }
       return name;
   }

    public static String UNION = "$Union$";
    public static String PARALLEL = "$Parallel$";
    public static String UNPARALLEL = "$Unparallel$";
    public static String LOW_LATENCY = "$LowLatency$";
    public static String END_LOW_LATENCY = "$EndLowLatency$";
    public static String ISOLATE = "$Isolate$";
    
    public BOutput lowLatency(BOutput parent){
        BOutput lowLatencyOutput = addPassThroughMarker(parent, LOW_LATENCY, true);
        return lowLatencyOutput;
    }
    
    public BOutput endLowLatency(BOutput parent){
        return addPassThroughMarker(parent, END_LOW_LATENCY, false);
    }
    
    public BOutput isolate(BOutput parent){
        return addPassThroughMarker(parent, ISOLATE, false);
    }

    public BOutput addUnion(Set<BOutput> outputs) {
        BOperator op = addVirtualMarkerOperator(UNION);
        return new BUnionOutput(op, outputs);
    }

    /**
     * Add a marker operator, that is actually a PassThrough in OperatorGraph,
     * so that we can run this graph locally with a single thread.
     */
    public BOutput parallel(BOutput parallelize, int width) {
    	BOutput parallelOutput = addPassThroughMarker(parallelize, PARALLEL, true);
    	parallelOutput.json().put("width", width);
        return parallelOutput;
    }

    /**
     * Add a marker operator, that is actually a PassThrough in OperatorGraph,
     * so that we can run this graph locally with a single thread.
     */
    public BOutput unparallel(BOutput parallelize) {
        return addPassThroughMarker(parallelize, UNPARALLEL, false);
    }

    public BOutput addPassThroughMarker(BOutput output, String kind,
            boolean createRegion) {
        BOperatorInvocation op = addOperator(PassThrough.class, null);
        op.json().put("marker", true);
        op.json().put("kind", kind);

        if (createRegion) {
            final String regionName = op.op().getName();
            regionMarkers.put(regionName, kind);
            op.addRegion(regionName);
        }

        // Create the input port that consumes the output
        BInputPort input = op.inputFrom(output, null);

        // Create the output port.
        return op.addOutput(input.port().getStreamSchema());
    }
    
    public BOutput addPassThroughOperator(BOutput output) {
        BOperatorInvocation op = addOperator(PassThrough.class, null);
        // Create the input port that consumes the output
        BInputPort input = op.inputFrom(output, null);
        // Create the output port.
        return op.addOutput(input.port().getStreamSchema());
    }

    public BOperator addVirtualMarkerOperator(String kind) {
        final BMarkerOperator op = new BMarkerOperator(this, kind);
        ops.add(op);
        return op;
    }

    public BOperatorInvocation addSPLOperator(String kind,
            Map<String, ? extends Object> params) {
        final BOperatorInvocation op = new BOperatorInvocation(this, params);
        op.json().put("kind", kind);
        op.json().put("runtime", "spl.cpp");
        ops.add(op);
        return op;
    }
    public BOperatorInvocation addSPLOperator(String name, String kind,
            Map<String, ? extends Object> params) {
        name = userSuppliedName(name);
        final BOperatorInvocation op = new BOperatorInvocation(this, name, params);
        op.json().put("kind", kind);
        op.json().put("runtime", "spl.cpp");
        ops.add(op);
        return op;
    }
    
    /**
     * @throws IllegalStateException if the topology can't run in 
     *          StreamsContext.Type.EMBEDDED mode.
     */
    public void checkSupportsEmbeddedMode() throws IllegalStateException {
        for (BOperator op : ops) {
            // note: runtime==null for markers
            String runtime = (String) op.json().get("runtime");
            if (!"spl.java".equals(runtime)) {
                Boolean marker = (Boolean) op.json().get("marker");
                if (marker==null || !marker) {
                    String namespace = (String) json().get("namespace");
                    String name = (String) json().get("name");
                    throw new IllegalStateException(
                            "Topology '"+namespace+"."+name+"'"
                            + " does not support "+StreamsContext.Type.EMBEDDED+" mode:"
                            + " the topology contains non-Java operators.");
                }
            }
        }
    }

    private Map<String, String> regionMarkers = new HashMap<>();

    public String getRegionMarker(String name) {
        return regionMarkers.get(name);
    }

    public OperatorGraph graph() {
        return graph;
    }
    
    public JSONObject getConfig() {
        return config;
    }

    @Override
    public JSONObject complete() {
        JSONObject json = json();
        JSONArray oa = new JSONArray(ops.size());
        for (BOperator op : ops) {
            oa.add(op.complete());
        }

        json.put("operators", oa);

        return json;
    }

    /**
     * @return the ops
     */
    public List<BOperator> getOps() {
        return ops;
    }
}
