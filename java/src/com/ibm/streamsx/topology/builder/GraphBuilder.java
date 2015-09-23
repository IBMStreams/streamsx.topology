/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.builder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.ibm.streamsx.topology.builder.BVirtualMarker.END_LOW_LATENCY;
import static com.ibm.streamsx.topology.builder.BVirtualMarker.LOW_LATENCY;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.json.java.OrderedJSONObject;
import com.ibm.streams.flow.declare.OperatorGraph;
import com.ibm.streams.flow.declare.OperatorGraphFactory;
import com.ibm.streams.operator.Operator;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.function.Consumer;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.generator.spl.GraphUtilities;
import com.ibm.streamsx.topology.generator.spl.GraphUtilities.Direction;
import com.ibm.streamsx.topology.generator.spl.GraphUtilities.VisitController;
import com.ibm.streamsx.topology.generator.spl.SubmissionTimeValue;
import com.ibm.streamsx.topology.internal.functional.ops.PassThrough;
import com.ibm.streamsx.topology.tuple.JSONAble;

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
    
    private final JSONObject config = new OrderedJSONObject();

    private final JSONObject params = new OrderedJSONObject();
    private final JSONObject spParams = new JSONObject();
    
    public GraphBuilder(String namespace, String name) {
        super();

        json().put("namespace", namespace);
        json().put("name", name);
        json().put("public", true);
        json().put("config", config);
        json().put("parameters", params);
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
        JSONObject graph = complete();
        final VisitController visitController =
                new VisitController(Direction.UPSTREAM);
        final int[] openRegionCount = { 0 };
        for (BOperator operator : operators) {
            JSONObject jop = operator.complete();
            GraphUtilities.visitOnce(visitController,
                    Collections.singletonList(jop), graph,
                new Consumer<JSONObject>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public void accept(JSONObject jo) {
                        String kind = (String) jo.get("kind");
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
            parallelOutput.json().put("width", width.get());
        else
            parallelOutput.json().put("width", ((JSONAble) width).toJSON());
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
        BOperatorInvocation op = addOperator(PassThrough.class, null);
        op.json().put("marker", true);
        op.json().put("kind", virtualMarker.kind());

        if (createRegion) {
            final String regionName = op.op().getName();
            regionMarkers.put(regionName, virtualMarker.kind());
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

    public BOperator addVirtualMarkerOperator(BVirtualMarker kind) {
        final BMarkerOperator op = new BMarkerOperator(this, kind);
        ops.add(op);
        return op;
    }

    public BOperatorInvocation addSPLOperator(String kind,
            Map<String, ? extends Object> params) {
        final BOperatorInvocation op = new BOperatorInvocation(this, params);
        op.json().put("kind", kind);

        json().put(JOperator.MODEL, JOperator.MODEL_SPL);
        json().put(JOperator.LANGUAGE, JOperator.LANGUAGE_SPL);

        ops.add(op);
        return op;
    }
    public BOperatorInvocation addSPLOperator(String name, String kind,
            Map<String, ? extends Object> params) {
        name = userSuppliedName(name);
        final BOperatorInvocation op = new BOperatorInvocation(this, name, params);
        op.json().put("kind", kind);
        
        json().put(JOperator.MODEL, JOperator.MODEL_SPL);
        json().put(JOperator.LANGUAGE, JOperator.LANGUAGE_SPL);
        
        ops.add(op);
        return op;
    }
    
    /**
     * @throws IllegalStateException if the topology can't run in 
     *          StreamsContext.Type.EMBEDDED mode.
     */
    public void checkSupportsEmbeddedMode() throws IllegalStateException {
        for (BOperator op : ops) {
            if (BVirtualMarker.isVirtualMarker((String) op.json().get("kind")))
                continue;
            
            // note: runtime==null for markers
            String runtime = (String) op.json().get(JOperator.MODEL);
            String language = (String) op.json().get(JOperator.LANGUAGE);
            
            if (!JOperator.MODEL_SPL.equals(runtime) || !JOperator.LANGUAGE_JAVA.equals(language)) {
                    String namespace = (String) json().get("namespace");
                    String name = (String) json().get("name");
                    throw new IllegalStateException(
                            "Topology '"+namespace+"."+name+"'"
                            + " does not support "+StreamsContext.Type.EMBEDDED+" mode:"
                            + " the topology contains non-Java operator:" + op.json().get("kind"));
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
    public void createSubmissionParameter(String name, JSONObject jo) {
        if (spParams.containsKey(name))
            throw new IllegalArgumentException("name is already defined");
        spParams.put(name, jo);
        params.put(SubmissionTimeValue.mkOpParamName(name), jo);
    }

}
