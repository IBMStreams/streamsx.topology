/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.builder;

import static com.ibm.streamsx.topology.builder.BVirtualMarker.END_LOW_LATENCY;
import static com.ibm.streamsx.topology.builder.BVirtualMarker.LOW_LATENCY;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.KIND_CLASS;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.LANGUAGE_JAVA;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.LANGUAGE_SPL;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.MODEL_SPL;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.MODEL_VIRTUAL;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.CFG_STREAMS_VERSION;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.NAME;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.NAMESPACE;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.spi.builder.Properties.Graph.CONFIG;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.function.Consumer;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.generator.spl.GraphUtilities;
import com.ibm.streamsx.topology.generator.spl.GraphUtilities.Direction;
import com.ibm.streamsx.topology.generator.spl.GraphUtilities.VisitController;
import com.ibm.streamsx.topology.internal.core.JavaFunctionalOps;
import com.ibm.streamsx.topology.internal.core.SubmissionParameter;
import com.ibm.streamsx.topology.internal.streams.Util;

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
 * 
 */
public class GraphBuilder extends BJSONObject {

    private final List<BOperator> ops = new ArrayList<>();
    
    private final JsonObject config = new JsonObject();

    /**
     * Submission parameters.
     */
    private final JsonObject params = new JsonObject();
    
    public GraphBuilder(String namespace, String name) {
        super();

        _json().addProperty(NAMESPACE, namespace);
        _json().addProperty(NAME, name);
        _json().addProperty("public", true);
        _json().add(CONFIG, config);
        _json().add("parameters", params);

        getConfig().addProperty(CFG_STREAMS_VERSION, Util.productVersion());
        ;
    }
   
   private final Map<String,Integer> usedNames = new HashMap<>();
   
   public BOperatorInvocation addOperator(
           String name,
           String kind,         
           Map<String, ? extends Object> params) {
       
       name = userSuppliedName(name);
       
       final BOperatorInvocation op = new BOperatorInvocation(this, name, kind,
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
    
    public BOutput lowLatency(BOutput parent){
        BOutput lowLatencyOutput = addPassThroughMarker(parent, BVirtualMarker.LOW_LATENCY, true);
        return lowLatencyOutput;
    }
    
    public BOutput endLowLatency(BOutput parent){
        return addPassThroughMarker(parent, BVirtualMarker.END_LOW_LATENCY, false);
    }

    public boolean isInLowLatencyRegion(BOutput output) {
        BOperator op;
        if (output instanceof BUnionOutput)
            op = ((BUnionOutput) output).operator();
        else
            op = ((BOutputPort) output).operator();
        return isInLowLatencyRegion(op);
    }
    
    public boolean isInLowLatencyRegion(BOperator... operators) {
        // handle nested low latency regions
        JsonObject graph = _complete();
        final VisitController visitController =
                new VisitController(Direction.UPSTREAM);
        final int[] openRegionCount = { 0 };
        for (BOperator operator : operators) {
            JsonObject jop = operator._complete();
            GraphUtilities.visitOnce(visitController,
                    Collections.singleton(jop), graph,
                new Consumer<JsonObject>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public void accept(JsonObject jo) {
                        String kind = jstring(jo, "kind");
                        if (LOW_LATENCY.kind().equals(kind)) {
                            if (openRegionCount[0] <= 0)
                                visitController.setStop();
                            else
                                openRegionCount[0]--;
                        }
                        else if (END_LOW_LATENCY.kind().equals(kind))
                            openRegionCount[0]++;
                    }
                });
            if (visitController.stopped() || openRegionCount[0] > 0)
                return true;
        }
        return false;
    }
    
    public BOutput isolate(BOutput parent){
        return addPassThroughMarker(parent, BVirtualMarker.ISOLATE, false);
    }
    public BOutput autonomous(BOutput parent){
        return addPassThroughMarker(parent, BVirtualMarker.AUTONOMOUS, false);
    }

    public BOutput addUnion(Set<BOutput> outputs) {
        BOperator op = addVirtualMarkerOperator(BVirtualMarker.UNION);
        return new BUnionOutput(op, outputs);
    }

    /**
     * Add a marker operator, that is actually a PassThrough in OperatorGraph,
     * so that we can run this graph locally with a single thread.
     */
    public BOutput parallel(BOutput parallelize, Supplier<Integer> width) {
        BOutput parallelOutput = addPassThroughMarker(parallelize, BVirtualMarker.PARALLEL, true);
        if (width.get() != null)
            parallelOutput._json().addProperty("width", width.get());
        else {
            SubmissionParameter<?> spw = (SubmissionParameter<?>) width;
            parallelOutput._json().add("width", spw.asJSON());
        }
        return parallelOutput;
    }

    /**
     * Add a marker operator, that is actually a PassThrough in OperatorGraph,
     * so that we can run this graph locally with a single thread.
     */
    public BOutput unparallel(BOutput parallelize) {
        return addPassThroughMarker(parallelize, BVirtualMarker.END_PARALLEL, false);
    }

    public BOutput addPassThroughMarker(BOutput output, BVirtualMarker virtualMarker,
            boolean createRegion) {
        BOperatorInvocation op = addOperator(virtualMarker.name(), virtualMarker.kind(), null);
        op._json().addProperty("marker", true);
        op._json().addProperty(KIND_CLASS, JavaFunctionalOps.PASS_CLASS);
        op.setModel(MODEL_VIRTUAL, LANGUAGE_JAVA);

        if (createRegion) {
            final String regionName = op.name();
            regionMarkers.put(regionName, virtualMarker.kind());
            op.addRegion(regionName);
        }

        // Create the input port that consumes the output
        BInputPort input = op.inputFrom(output, null);

        // Create the output port.
        return op.addOutput(input._schema());
    }
    
    public BOutput addPassThroughOperator(BOutput output) {
        BOperatorInvocation op = addOperator("Pass", JavaFunctionalOps.PASS_KIND, null);
        op.setModel(MODEL_SPL, LANGUAGE_JAVA);
        // Create the input port that consumes the output
        BInputPort input = op.inputFrom(output, null);
        // Create the output port.
        return op.addOutput(input._schema());
    }

    public BOperator addVirtualMarkerOperator(BVirtualMarker kind) {
        final BMarkerOperator op = new BMarkerOperator(this, kind);
        ops.add(op);
        return op;
    }

    public BOperatorInvocation addSPLOperator(String kind,
            Map<String, ? extends Object> params) {
        
        String name = kind.contains("::") ?
                kind.substring(kind.lastIndexOf("::" + 2), kind.length()) :
                    kind;
         return addSPLOperator(name, kind, params);
    }
    public BOperatorInvocation addSPLOperator(String name, String kind,
            Map<String, ? extends Object> params) {
        name = userSuppliedName(name);
        final BOperatorInvocation op = new BOperatorInvocation(this, name, kind, params);      
        op.setModel(MODEL_SPL, LANGUAGE_SPL);
        
        ops.add(op);
        return op;
    }

    private Map<String, String> regionMarkers = new HashMap<>();

    public String getRegionMarker(String name) {
        return regionMarkers.get(name);
    }
    
    public JsonObject getConfig() {
        return config;
    }
    
    public JsonObject _complete() {
        JsonObject json = super._complete();
        
        JsonArray oa = new JsonArray();
        json.add("operators", oa);
        for (BOperator op : ops) {
            oa.add(op._complete());
        }
                
        return json;
    }

    /**
     * @return the ops
     */
    public List<BOperator> getOps() {
        return ops;
    }

    /**
     * Create a submission parameter.
     * <p>  
     * The SubmissionParameter parameter value json is: 
     * <pre><code>
     * object {
     *   type : "submissionParameter"
     *   value : object {
     *     name : string. submission parameter name
     *     metaType : com.ibm.streams.operator.Type.MetaType.name() string
     *     defaultValue : any. may be null. type appropriate for metaType
     *   }
     * }
     * </code></pre>
     * @param name the submission parameter name
     * @param jo the SubmissionParameter parameter value object
     */
    public void createSubmissionParameter(String name, JsonObject jo) {
        if (params.has(name))
            throw new IllegalArgumentException("name is already defined");
        params.add(name, jo);
    }
}
