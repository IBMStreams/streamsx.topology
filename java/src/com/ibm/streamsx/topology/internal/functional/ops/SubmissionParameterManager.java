/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional.ops;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streamsx.topology.builder.GraphBuilder;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.generator.spl.SubmissionTimeValue;
import static com.ibm.streamsx.topology.internal.core.SubmissionParameter.TYPE_SUBMISSION_PARAMETER;

/**
 * A manager for making a submission parameter Supplier usable
 * from functional logic.
 * <p>
 * The manager maintains a collection of values for every submission parameter
 * in the graph.
 * <p>
 * The submission parameter's {@code Supplier.get()} implementation learns
 * the actual submission parameter value by calling
 * {@link #getValue(String, MetaType)}.
 * <p>
 * The strategy for the manager learning the submission parameter values
 * is as follows.
 * <p>
 * For DISTRIBUTED and STANDALONE (SPL code) submission parameter
 * values are conveyed via a NAME_SUBMISSION_PARAMS functional operator
 * parameter.  The parameter's value consists of all the submission parameters
 * created in the graph.  All functional operators have the same
 * value for this parameter in their OperatorContext.  Functional operators
 * call {@link #initialize(OperatorContext)}.
 * <p>
 * For EMBEDDED the functional operator's operator context does not have
 * a NAME_SUBMISSION_PARAMS parameter.  Instead the EMBEDDED
 * {@code StreamsContext.submit()} calls
 * {@link #initializeEmbedded(GraphBuilder, Map)}.
 * The collection of all submission parameters, with optional default values,
 * are learned from the graph and actual values are learned from the
 * submit configuration's {@link ContextProperties#SUBMISSION_PARAMS} value.
 */
public class SubmissionParameterManager {
    private static abstract class Factory {
        abstract Object valueOf(String s);
    }
    private static Map<MetaType,Factory> factories = new HashMap<>();
    static {
        // TODO more
        factories.put(MetaType.RSTRING, new Factory() {
            Object valueOf(String s) { return s; } });
        factories.put(MetaType.USTRING, new Factory() {
            Object valueOf(String s) { return s; } });
        factories.put(MetaType.INT8, new Factory() {
            Object valueOf(String s) { return Byte.valueOf(s); } });
        factories.put(MetaType.INT16, new Factory() {
            Object valueOf(String s) { return Short.valueOf(s); } });
        factories.put(MetaType.INT32, new Factory() {
            Object valueOf(String s) { return Integer.valueOf(s); } });
        factories.put(MetaType.INT64, new Factory() {
            Object valueOf(String s) { return Long.valueOf(s); } });
        factories.put(MetaType.UINT8, new Factory() {
            Object valueOf(String s) { return Byte.valueOf(s); } });
        factories.put(MetaType.UINT16, new Factory() {
            Object valueOf(String s) { return Short.valueOf(s); } });
        factories.put(MetaType.UINT32, new Factory() {
            Object valueOf(String s) { return Integer.valueOf(s); } });
        factories.put(MetaType.UINT64, new Factory() {
            Object valueOf(String s) { return Long.valueOf(s); } });
        factories.put(MetaType.FLOAT32, new Factory() {
            Object valueOf(String s) { return Float.valueOf(s); } });
        factories.put(MetaType.FLOAT64, new Factory() {
            Object valueOf(String s) { return Double.valueOf(s); } });
    }
    private static final Map<String,String> UNINIT_MAP = Collections.emptyMap();
    /**  map of topology's <spOpParamName, strVal> */
    private static volatile Map<String,String> params = UNINIT_MAP;
    
    /** The name of the functional operator's actual SPL parameter
     * for the submission parameters names */
    public static final String NAME_SUBMISSION_PARAM_NAMES = "submissionParamNames";
    /** The name of the functional operator's actual SPL parameter
     * for the submission parameters values */
    public static final String NAME_SUBMISSION_PARAM_VALUES = "submissionParamValues";
    
    /**
     * Initialize submission parameter value information
     * from operator context information.
     * @param context the operator context
     */
    public synchronized static void initialize(OperatorContext context) {
        // The TYPE_SPL_SUBMISSION_PARAMS parameter value is the same for
        // all operator contexts.
        if (params == UNINIT_MAP) {
            List<String> names = context.getParameterValues(NAME_SUBMISSION_PARAM_NAMES);
            if (names != null && !names.isEmpty()) {
                List<String> values = context.getParameterValues(NAME_SUBMISSION_PARAM_VALUES);
                Map<String,String> map = new HashMap<>();
                for (int i = 0; i < names.size(); i++) {
                    String name = names.get(i);
                    String value = values.get(i);
                    map.put(name, value);
                }
                params = map;
                // System.out.println("SPM.initialize() " + params);
            }
        }
    }

    /**
     * Initialize EMBEDDED submission parameter value information
     * from topology's graph and StreamsContext.submit() config.
     * @param builder the topology's builder
     * @param config StreamsContext.submit() configuration
     */
    public synchronized static void initializeEmbedded(GraphBuilder builder,
            Map<String, Object> config) {

        // N.B. in an embedded context, within a single JVM/classloader,
        // multiple topologies can be executed serially as well as concurrently.
        // TODO handle the concurrent case - e.g., with per-topology-submit
        // managers.
        
        // create map of all submission params used by the topology
        // and the parameter's string value (initially null for no default)
        Map<String,String> allsp = new HashMap<>();  // spName, spStrVal
        
        JSONObject graph = builder.json();
        JSONObject gparams = (JSONObject) graph.get("parameters");
        if (gparams != null) {
            for (Object o : gparams.keySet()) {
                JSONObject param = (JSONObject) gparams.get((String)o);
                if (TYPE_SUBMISSION_PARAMETER.equals(param.get("type"))) {
                    JSONObject spval = (JSONObject) param.get("value");
                    Object val = spval.get("defaultValue");
                    if (val != null)
                        val = val.toString();
                    allsp.put((String)spval.get("name"), (String)val);
                }
            }
        }
        if (allsp.isEmpty())
            return;
        
        // update values from config
        @SuppressWarnings("unchecked")
        Map<String,Object> spValues =
            (Map<String, Object>) config.get(ContextProperties.SUBMISSION_PARAMS);
        for (String spName : spValues.keySet()) {
            if (allsp.containsKey(spName)) {
                Object val = spValues.get(spName);
                if (val != null)
                    val = val.toString();
                allsp.put(spName, (String)val);
            }
        }
        
        // failure if any are still undefined
        for (String spName : allsp.keySet()) {
            if (allsp.get(spName) == null)
                throw new IllegalStateException("Submission parameter \""+spName+"\" requires a value but none has been supplied");
        }
        
        // good to go. initialize params
        params = new HashMap<>();
        for (Map.Entry<String, String> e : allsp.entrySet()) {
            params.put(SubmissionTimeValue.mkOpParamName(e.getKey()), e.getValue());
        }
        // System.out.println("SPM.initializeEmbedded() " + params);
    }

    /**
     * Get the submission parameter's value.
     * <p>
     * The value will be null while composing the topology.  It will be
     * the actual submission time value when the topology is running.
     * @param spName submission parameter name
     * @param metaType parameter's metaType
     * @return the parameter's value appropriately typed for metaType.
     *          may be null.
     */
    public static Object getValue(String spName, MetaType metaType) {
        String value = params.get(SubmissionTimeValue.mkOpParamName(spName));
        if (value == null) {
            // System.out.println("SPM.getValue "+spName+" "+metaType+ " params " + params);
            throw new IllegalArgumentException("Unexpected submission parameter name " + spName);
        }
        Factory factory = factories.get(metaType);
        if (factory == null)
            throw new IllegalArgumentException("Unhandled MetaType " + metaType);
        return factory.valueOf(value);
    }

}
